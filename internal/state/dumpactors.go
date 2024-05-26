package state

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/bits"
	"sync"

	"code.riba.cloud/go/toolbox-interplanetary/fil"
	"code.riba.cloud/go/toolbox/cmn"
	filexp "github.com/aschmahmann/filexp/internal"
	"github.com/aschmahmann/filexp/internal/ipld"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/multiformats/go-varint"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	// all of this should be in go-state-types, but alas...
	lchadt "github.com/filecoin-project/lotus/chain/actors/adt"
	lchbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lchbiacct "github.com/filecoin-project/lotus/chain/actors/builtin/account"
	lchbidc "github.com/filecoin-project/lotus/chain/actors/builtin/datacap"
	lchbiinit "github.com/filecoin-project/lotus/chain/actors/builtin/init"
	lchbiminer "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	lchbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchbipower "github.com/filecoin-project/lotus/chain/actors/builtin/power"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
)

func DumpActorStateSQLite(ctx context.Context, bg *ipld.CountingBlockGetter, ts *fil.LotusTS, outFh uintptr, withEth bool) (defErr error) {

	// the state is not exactly... clean
	// keep track of actors encountered in different iterations to catch pointer mismatches
	//
	// positive count - encounters in actor state, during the StateTree.Actors(HAMT[ActorID]Actor) iteration
	// negative count - encounters as key in the reverse-map InitActor(f01).State.AddressMap(HAMT[Address]ActorID)
	//
	var mu sync.Mutex
	type addrTuple struct {
		actorID uint64
		robust  filaddr.Address
	}
	type encounterCount map[addrTuple]int
	delegatedAddrsf01Map := make(encounterCount, 4<<20)
	accountActorReverseMap := make(encounterCount, 4<<20)

	stLib, finalizer, err := initDB(ctx, outFh, withEth)
	if err != nil {
		return cmn.WrErr(err)
	}
	defer func() {
		bg.LogStats()

		for _, encmap := range []encounterCount{delegatedAddrsf01Map, accountActorReverseMap} {
			for t, cnt := range encmap {
				if cnt < 0 {
					defErr = multierr.Append(defErr, cmn.WrErr(
						stLib.execSt(insMartians,
							"pubkey entry in InitActor(f01) without any actor state references",
							t.robust.String(),
							fmt.Sprintf("InitActor(f01).State.AddressMap(HAMT[Address]ActorID), key 0x%X, value %d", t.robust.Bytes(), t.actorID),
						),
					))
				} else if cnt > 0 {
					ai, _ := filaddr.NewIDAddress(t.actorID)
					// builtin singletons are expected to be self-referential
					if t.actorID >= filbuiltin.FirstNonSingletonActorId ||
						ai.String() != t.robust.String() {
						defErr = multierr.Append(defErr, cmn.WrErr(
							stLib.execSt(insMartians,
								"pubkey entry in actor state without corresponding InitActor(f01) reference",
								t.robust.String(),
								fmt.Sprintf("State of Actor(f0%d)", t.actorID),
							),
						))
					}
				}
			}
		}

		if defErr == nil {
			defErr = finalizer()
		}
	}()

	cbs := ipldcbor.NewCborStore(bg)
	eg, ctx := errgroup.WithContext(ctx)

	// IterLoop_1 goes through the state tree keyed off address IDs, get balances + other metadata
	eg.Go(func() error {
		// t0 := time.Now()
		// defer func() {
		// 	log.Infof("1 done after %s", time.Since(t0).Truncate(time.Millisecond).String())
		// }()

		sr, err := LoadStateRoot(ctx, cbs, ts)
		if err != nil {
			return cmn.WrErr(err)
		}

		return ipld.ParallelIterateMap(ctx, cbs, sr.Actors, func(k, vCbor []byte) error {
			if len(k) == 0 || k[0] != 0 {
				return xerrors.Errorf("unexpected ActorID key %X", k)
			}
			actorID, _, err := varint.FromUvarint(k[1:])
			if err != nil {
				return cmn.WrErr(err)
			}

			var act lchtypes.Actor
			if err := (&act).UnmarshalCBOR(bytes.NewReader(vCbor)); err != nil {
				return xerrors.Errorf("unable to unmarshal actor state %X: %w", vCbor, err)
			}

			if err := stLib.execSt(insActors, actorID, act.Nonce, act.Balance.String(), act.Code.String()); err != nil {
				return cmn.WrErr(err)
			}
			if withEth {
				if err := stLib.execSt(insEth, ethtypes.EthAddressFromActorID(filabi.ActorID(actorID)).String(), actorID); err != nil {
					return cmn.WrErr(err)
				}
			}

			if act.DelegatedAddress != nil {
				// keep track of encountered delegates to match against f01
				mu.Lock()
				delegatedAddrsf01Map[addrTuple{actorID, *act.DelegatedAddress}]++
				mu.Unlock()

				// only write out invalid entries, valid lookups written in the other loop
				if _, castErr := ethtypes.EthAddressFromFilecoinAddress(*act.DelegatedAddress); castErr != nil {
					if err := stLib.execSt(insMartians,
						castErr.Error(),
						act.DelegatedAddress.String(),
						fmt.Sprintf("StateTree.Actors(HAMT[ActorID]Actor), key 0x%X (f0%d), value 0x%X, field DelegatedAddress", k, actorID, vCbor),
					); err != nil {
						return cmn.WrErr(err)
					}
				}
			}

			// further parse some builtin actors
			switch {

			//
			// Account actors (f1 + f3), just record the encounter
			//
			case lchbi.IsAccountActor(act.Code):
				aa, err := lchbiacct.Load(lchadt.WrapStore(ctx, cbs), &act)
				if err != nil {
					return cmn.WrErr(err)
				}
				pk, err := aa.PubkeyAddress()
				if err != nil {
					return cmn.WrErr(err)
				}

				mu.Lock()
				accountActorReverseMap[addrTuple{actorID, pk}]++
				mu.Unlock()

				if actorID < filbuiltin.FirstNonSingletonActorId {
					faid, _ := filaddr.NewIDAddress(actorID)
					if pk.String() != faid.String() {
						if err := stLib.execSt(insMartians,
							"singleton actor of type 'account' unexpectedly contains real PubkeyAddress",
							pk.String(),
							fmt.Sprintf("State of AccountActor(f0%d), field PubkeyAddress", actorID),
						); err != nil {
							return cmn.WrErr(err)
						}
					}
				} else {
				}

			//
			// Fil-builtin Multisig actor
			//
			case lchbi.IsMultisigActor(act.Code):
				ms, err := lchbimsig.Load(lchadt.WrapStore(ctx, cbs), &act)
				if err != nil {
					return cmn.WrErr(err)
				}

				mss, err := ms.Signers()
				if err != nil {
					return cmn.WrErr(err)
				}
				for _, s := range mss {
					sid, err := filaddr.IDFromAddress(s)
					if err != nil {
						return xerrors.Errorf("unexpected msig signer %s: %w", s.String(), err)
					}
					if err := stLib.execSt(insMsigSigners, actorID, sid); err != nil {
						return cmn.WrErr(err)
					}
				}

				mst, err := ms.Threshold()
				if err != nil {
					return cmn.WrErr(err)
				}

				if mst > uint64(len(mss)) {
					if err := stLib.execSt(insMartians,
						"multisig with unattainable threshold",
						fmt.Sprintf("f0%d", actorID),
						fmt.Sprintf("MultisigActor(f0%d) with threshold %d yet %d signers", actorID, mst, len(mss)),
					); err != nil {
						return cmn.WrErr(err)
					}
				}

				msib, err := ms.InitialBalance()
				if err != nil {
					return cmn.WrErr(err)
				}
				msse, err := ms.StartEpoch()
				if err != nil {
					return cmn.WrErr(err)
				}
				msud, err := ms.UnlockDuration()
				if err != nil {
					return cmn.WrErr(err)
				}

				if err := stLib.execSt(insMsigs, actorID, mst, msib.String(), msse, msud); err != nil {
					return cmn.WrErr(err)
				}

			//
			// Miner Actor
			//
			case lchbi.IsStorageMinerActor(act.Code):
				ma, err := lchbiminer.Load(lchadt.WrapStore(ctx, cbs), &act)
				if err != nil {
					return cmn.WrErr(err)
				}
				mi, err := ma.Info()
				if err != nil {
					return cmn.WrErr(err)
				}
				lf, err := ma.LockedFunds()
				if err != nil {
					return cmn.WrErr(err)
				}
				var spa [3]uint64
				for i, a := range []filaddr.Address{mi.Owner, mi.Beneficiary, mi.Worker} {
					aid, err := filaddr.IDFromAddress(a)
					if err != nil {
						return xerrors.Errorf("unexpected SP f0%d controlling address #%d %s: %w", actorID, i, a.String(), err)
					}
					spa[i] = aid
				}

				if err := stLib.execSt(insSPs,
					actorID,
					spa[0], spa[1], spa[2],
					bits.TrailingZeros64(uint64(mi.SectorSize)),
					lf.InitialPledgeRequirement.String(),
					lf.VestingFunds.String(),
				); err != nil {
					return cmn.WrErr(err)
				}
			}

			return nil
		})
	})

	// IterLoop_2 goes through the f01 actor state, populates the reverse robust => ID index
	eg.Go(func() error {
		// t0 := time.Now()
		// defer func() {
		// 	log.Infof("2 done after %s", time.Since(t0).Truncate(time.Millisecond).String())
		// }()

		f01act, err := GetActorGeneric(cbs, ts, filbuiltin.InitActorAddr)
		if err != nil {
			return cmn.WrErr(err)
		}

		f01state, err := lchbiinit.Load(lchadt.WrapStore(ctx, cbs), f01act)
		if err != nil {
			return cmn.WrErr(err)
		}
		f01mapRoot, err := cidFromStateByFieldpath(f01state, "AddressMap")
		if err != nil {
			return cmn.WrErr(err)
		}

		return ipld.ParallelIterateMap(ctx, cbs, f01mapRoot, func(k, vCbor []byte) error {
			robust, err := filaddr.NewFromBytes(k)
			if err != nil {
				return cmn.WrErr(err)
			}

			var actorID cbg.CborInt
			if err := actorID.UnmarshalCBOR(bytes.NewReader(vCbor)); err != nil {
				return cmn.WrErr(err)
			}

			if err := stLib.execSt(insRobusts, robust.String(), actorID); err != nil {
				return cmn.WrErr(err)
			}

			switch robust.Protocol() {

			case filaddr.SECP256K1, filaddr.BLS:
				// make sure we have all account actors mapped both ways
				mu.Lock()
				accountActorReverseMap[addrTuple{uint64(actorID), robust}]--
				mu.Unlock()

			case filaddr.Delegated:
				// keep track of encountered delegates to match against delegated entries
				mu.Lock()
				delegatedAddrsf01Map[addrTuple{uint64(actorID), robust}]--
				mu.Unlock()

				ea, castErr := ethtypes.EthAddressFromFilecoinAddress(robust)
				if castErr != nil {
					if err := stLib.execSt(insMartians,
						castErr.Error(),
						robust.String(),
						fmt.Sprintf("InitActor(f01).State.AddressMap(HAMT[Address]ActorID), key 0x%X, value %d", k, actorID),
					); err != nil {
						return cmn.WrErr(err)
					}
				} else if withEth {
					if err := stLib.execSt(insEth, ea.String(), actorID); err != nil {
						return cmn.WrErr(err)
					}
				}
			}

			return nil
		})
	})

	// IterLoop_3 goes through the f07 DC actor state, find where all the potential staking licenses live
	denom18 := filbig.NewInt(1_000_000_000_000_000_000)
	eg.Go(func() error {
		// t0 := time.Now()
		// defer func() {
		// 	log.Infof("3 done after %s", time.Since(t0).Truncate(time.Millisecond).String())
		// }()

		f07act, err := GetActorGeneric(cbs, ts, filbuiltin.DatacapActorAddr)
		if err != nil {
			return cmn.WrErr(err)
		}
		f07state, err := lchbidc.Load(lchadt.WrapStore(ctx, cbs), f07act)
		if err != nil {
			return cmn.WrErr(err)
		}
		f07mapRoot, err := cidFromStateByFieldpath(f07state, "Token", "Balances")
		if err != nil {
			return cmn.WrErr(err)
		}

		return ipld.ParallelIterateMap(ctx, cbs, f07mapRoot, func(k, vCbor []byte) error {
			id, _, err := varint.FromUvarint(k)
			if err != nil {
				return cmn.WrErr(err)
			}

			// for some reason DC values are stored as BYTESe18, because fractional bytes are totally a thing...
			var amt filbig.Int
			if err := (&amt).UnmarshalCBOR(bytes.NewReader(vCbor)); err != nil {
				return xerrors.Errorf("unable to unmarshal FilBigint value %X: %w", vCbor, err)
			}

			return cmn.WrErr(
				stLib.execSt(insDC, id, filbig.Div(amt, denom18).Int64()),
			)
		})
	})

	// IterLoop_4 goes through the f04 power actor state, find who has what
	// DO NOT use the parallel iterator, because exposed struct types are just shit...
	// ( it still *BARELY* finishes before the heavier loops, so all good )
	eg.Go(func() error {
		// t0 := time.Now()
		// defer func() {
		// 	log.Infof("4 done after %s", time.Since(t0).Truncate(time.Millisecond).String())
		// }()

		f04act, err := GetActorGeneric(cbs, ts, filbuiltin.StoragePowerActorAddr)
		if err != nil {
			return cmn.WrErr(err)
		}
		f04state, err := lchbipower.Load(lchadt.WrapStore(ctx, cbs), f04act)
		if err != nil {
			return cmn.WrErr(err)
		}

		return cmn.WrErr(f04state.ForEachClaim(func(spid filaddr.Address, claim lchbipower.Claim) error {
			id, err := filaddr.IDFromAddress(spid)
			if err != nil {
				return xerrors.Errorf("unexpected miner id in claim HAMT %s: %w", spid.String(), err)
			}
			if !claim.QualityAdjPower.IsInt64() {
				log.Warnf("claimed QAP for %s is unexpectedly large: %s", spid.String(), claim.QualityAdjPower.String())
			}
			return cmn.WrErr(
				stLib.execSt(insPower, id, claim.RawBytePower.String(), claim.QualityAdjPower.String()),
			)
		}, false))
	})

	return eg.Wait()
}

type statementLib map[insStatementKey]*sql.Stmt

func (sl statementLib) execSt(k insStatementKey, args ...any) error {
	// return nil
	_, err := sl[k].Exec(args...)
	return cmn.WrErr(err)
}

type insStatementKey int

const (
	insActors = insStatementKey(iota)
	insRobusts
	insMartians
	insDC
	insMsigs
	insMsigSigners
	insSPs
	insPower
	insEth
)

func initDB(ctx context.Context, outFh uintptr, withEth bool) (_ statementLib, _ func() error, defErr error) {

	db, err := sql.Open(filexp.DbDriverID, fmt.Sprintf("file:///dev/fd/%d", outFh))
	if err != nil {
		return nil, nil, cmn.WrErr(err)
	}

	inserters := map[insStatementKey]string{
		insActors:      `INSERT INTO tmp_actors				( actor_id, next_nonce, afil_balance, code_cid )										VALUES ( ?, ?, ?, ? )`,
		insRobusts:     `INSERT INTO tmp_robusts			( robust, actor_id )																								VALUES ( ?, ? )`,
		insMartians:    `INSERT INTO inconsistencies	( inconsistency, item, source )																			VALUES ( ?, ?, ? )`,
		insDC:          `INSERT INTO dc_balances			( actor_id, dc_balance )																						VALUES ( ?, ? )`,
		insMsigs:       `INSERT INTO fil_msigs 				( actor_id, threshold, afil_init_balance, lock_start, lock_epochs )	VALUES ( ?, ?, ?, ?, ? )`,
		insMsigSigners: `INSERT INTO fil_msig_signers ( actor_id, signer_id )																							VALUES ( ?, ? )`,
		insPower:       `INSERT INTO tmp_sp_power			(	actor_id, bytes_rbp, bytes_qap )																	VALUES ( ?, ?, ? )`,
		insSPs:         `INSERT INTO tmp_sps 					( actor_id, owner, beneficiary, worker, sector_log2_size, afil_pledge, afil_vesting )	VALUES ( ?, ?, ?, ?, ?, ?, ? )`,
	}

	if withEth {
		inserters[insEth] = `INSERT INTO tmp_eth_addrs ( eth_addr, actor_id ) VALUES ( ?, ? )`
	}

	initStatements := []string{
		// larger pagesize allows for stronger "k-anonymity" when querying remotely
		fmt.Sprintf(`PRAGMA page_size = %d`, 32<<10),

		// ( OFF or MEMORY is mandatory: impossible to open via a fileno otherwise )
		`PRAGMA journal_mode = OFF`,
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA synchronous = OFF`,
		`PRAGMA locking_mode = EXCLUSIVE`,

		`
		CREATE TABLE state(
			singleton_row BOOL PRIMARY KEY CONSTRAINT single_row_in_table CHECK ( singleton_row IS TRUE ),
			meta JSON NOT NULL
		) WITHOUT ROWID
		`,

		`INSERT INTO state ( singleton_row, meta ) VALUES ( true, '{ "schema_version":{ "major": 0, "minor": 8 } }' ) ON CONFLICT DO NOTHING`,

		`
		CREATE TABLE actor_types (
			actor_type_id INTEGER NOT NULL PRIMARY KEY,
			code_cid TEXT NOT NULL UNIQUE,
			label TEXT UNIQUE,
			singleton_state JSON
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE actors (
			actor_id INTEGER NOT NULL PRIMARY KEY,
			actor_type_id INTEGER NOT NULL REFERENCES actor_types ( actor_type_id ),
			next_nonce INTEGER NOT NULL,
			afil_balance TEXT NOT NULL
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE robusts (
			robust TEXT NOT NULL PRIMARY KEY,
			actor_id INTEGER NOT NULL REFERENCES actors ( actor_id )
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE eth_addrs (
			eth_addr TEXT NOT NULL PRIMARY KEY,
			actor_id INTEGER NOT NULL REFERENCES actors ( actor_id )
		) WITHOUT ROWID
		`,

		`
		CREATE TABLE fil_msigs (
			actor_id INTEGER NOT NULL PRIMARY KEY REFERENCES actors ( actor_id ),
			threshold SMALLINT NOT NULL,
			afil_init_balance TEXT NOT NULL,
			lock_start INTEGER NOT NULL,
			lock_epochs INTEGER NOT NULL
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE fil_msig_signers (
			actor_id INTEGER NOT NULL REFERENCES actors ( actor_id ),
			signer_id INTEGER NOT NULL REFERENCES actors ( actor_id ),
			PRIMARY KEY( actor_id, signer_id )
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE sps (
			actor_id INTEGER NOT NULL PRIMARY KEY REFERENCES actors ( actor_id ),
			owner INTEGER NOT NULL REFERENCES actors ( actor_id ),
			beneficiary INTEGER NOT NULL REFERENCES actors ( actor_id ),
			worker INTEGER NOT NULL REFERENCES actors ( actor_id ),
			afil_pledge TEXT NOT NULL,
			afil_vesting TEXT NOT NULL,
			bytes_rbp TEXT NOT NULL,
			bytes_qap TEXT NOT NULL,
			sector_log2_size SMALLINT NOT NULL
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE dc_balances (
			actor_id INTEGER NOT NULL PRIMARY KEY REFERENCES actors ( actor_id ),
			dc_balance BIGINT NOT NULL
		) WITHOUT ROWID
		`,
		`
		CREATE TABLE inconsistencies (
			inconsistency TEXT NOT NULL,
		  item TEXT NOT NULL,
			source TEXT NOT NULL,
			PRIMARY KEY( item, source )
		) WITHOUT ROWID
		`,

		// temp tables populated without indices/constraints at first
		`
		CREATE TABLE tmp_actors (
			actor_id INTEGER,
			next_nonce INTEGER,
			afil_balance TEXT,
			code_cid TEXT
		)
		`,
		`
		CREATE TABLE tmp_robusts (
			robust TEXT,
			actor_id INTEGER
		)
		`,
		`
		CREATE TABLE tmp_eth_addrs (
			eth_addr TEXT,
			actor_id INTEGER
		)
		`,
		`
		CREATE TABLE tmp_sps (
			actor_id INTEGER,
			owner INTEGER,
			beneficiary INTEGER,
			worker INTEGER,
			sector_log2_size SMALLINT,
			afil_pledge TEXT,
			afil_vesting TEXT
		)
		`,
		`
		CREATE TABLE tmp_sp_power (
			actor_id INTEGER,
			bytes_rbp TEXT,
			bytes_qap TEXT
		)
		`,
	}

	for _, s := range initStatements {
		if _, err := db.Exec(s); err != nil {
			log.Warn(err.Error())
			return nil, nil, xerrors.Errorf("schema init failed: error executing %s: %w", s, err)
		}
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadUncommitted})
	if err != nil {
		return nil, nil, cmn.WrErr(err)
	}

	stLib := make(statementLib, len(inserters))
	for k, s := range inserters {
		stLib[k], err = tx.Prepare(s)
		if err != nil {
			return nil, nil, cmn.WrErr(err)
		}
	}

	finalizer := func() error {
		log.Info("finalizing database...")

		eg, innerCtx := errgroup.WithContext(ctx)
		for _, statGroup := range [][]string{
			{
				`
				INSERT INTO actor_types ( actor_type_id, code_cid )
					SELECT MIN( actor_id ), code_cid FROM tmp_actors GROUP BY code_cid
				`,
				`
				INSERT INTO actors ( actor_id, actor_type_id, next_nonce, afil_balance )
					SELECT actor_id, actor_type_id, next_nonce, afil_balance
						FROM tmp_actors a
						LEFT JOIN actor_types at USING ( code_cid )
				`,
				`DROP TABLE tmp_actors`,
			},
			{
				`
				INSERT INTO robusts ( robust, actor_id )
					SELECT robust, actor_id FROM tmp_robusts
				`,
				`DROP TABLE tmp_robusts`,
			},
			{
				`
				INSERT INTO eth_addrs ( eth_addr, actor_id )
					SELECT eth_addr, actor_id FROM tmp_eth_addrs
				`,
				`DROP TABLE tmp_eth_addrs`,
			},
			{
				`
				INSERT INTO sps ( actor_id, owner, beneficiary, worker, afil_pledge, afil_vesting, bytes_rbp, bytes_qap, sector_log2_size )
					SELECT actor_id, owner, beneficiary, worker, afil_pledge, afil_vesting, bytes_rbp, bytes_qap, sector_log2_size
						FROM tmp_sps
						LEFT JOIN tmp_sp_power USING (actor_id)
				`,
				`DROP TABLE tmp_sps`,
				`DROP TABLE tmp_sp_power`,
			},
		} {
			eg.Go(func() error {
				for _, st := range statGroup {
					if _, err := tx.ExecContext(innerCtx, st); err != nil {
						log.Error(err)
						return xerrors.Errorf("failure executing %s: %w", st, err)
					}
				}
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			return cmn.WrErr(err)
		}

		// move all the unreferenced robusts as martians
		for _, st := range []string{
			`
			INSERT INTO inconsistencies ( inconsistency, item, source )
				SELECT
						"dangling pointer in (f01).AddressMap: value does not exist in StateTree.Actors",
						actor_id,
						"InitActor(f01).State.AddressMap(HAMT[Address]ActorID), key " || robust
					FROM robusts
					LEFT JOIN actors USING (actor_id)
				WHERE actors.actor_id IS NULL
			`,
			`DELETE FROM robusts WHERE actor_id NOT IN ( SELECT actor_id FROM actors )`,
		} {
			if _, err := tx.ExecContext(ctx, st); err != nil {
				log.Error(err)
				return xerrors.Errorf("failure executing %s: %w", st, err)
			}
		}

		return cmn.WrErr(tx.Commit())
	}

	return stLib, finalizer, nil
}
