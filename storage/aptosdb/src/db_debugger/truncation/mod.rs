// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    jellyfish_merkle_node::JellyfishMerkleNodeSchema,
    schema::{
        epoch_by_version::EpochByVersionSchema, ledger_info::LedgerInfoSchema,
        state_value::StateValueSchema, transaction::TransactionSchema, write_set::WriteSetSchema,
    },
    stale_node_index::StaleNodeIndexSchema,
    stale_node_index_cross_epoch::StaleNodeIndexCrossEpochSchema,
    stale_state_value_index::StaleStateValueIndexSchema,
    transaction_accumulator::TransactionAccumulatorSchema,
    transaction_info::TransactionInfoSchema,
    version_data::VersionDataSchema,
    AptosDB, EventStore, StateStore, TransactionStore,
};
use anyhow::Result;
use aptos_config::config::RocksdbConfigs;
use aptos_jellyfish_merkle::{node_type::NodeKey, StaleNodeIndex};
use aptos_schemadb::{
    schema::{Schema, SeekKeyCodec},
    ReadOptions, SchemaBatch, DB,
};
use aptos_types::transaction::Version;
use clap::Parser;
use std::{path::PathBuf, sync::Arc};

#[derive(Parser)]
#[clap(about = "Delete all data after the provided version.")]
pub struct Cmd {
    #[clap(long, parse(from_os_str))]
    db_dir: PathBuf,

    target_version: u64,

    ledger_db_batch_size: usize,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        let (ledger_db, state_merkle_db, _kv_db) = AptosDB::open_dbs(
            &self.db_dir,
            RocksdbConfigs::default(),
            /*readonly=*/ false,
        )?;

        // TODO(grao): Handle kv db once we enable it.

        let ledger_db_version = Self::get_current_version_in_ledger_db(&ledger_db)?
            .expect("Current version of ledger db must exist.");
        let state_merkle_db_version =
            Self::get_current_version_in_state_merkle_db(&state_merkle_db)?
                .expect("Current version of state merkle db must exist.");

        assert!(self.target_version <= ledger_db_version);
        assert!(ledger_db_version >= state_merkle_db_version);

        let state_merkle_target_version =
            Self::find_tree_root_at_or_before(&ledger_db, &state_merkle_db, self.target_version)?
                .expect(&format!(
                    "Could not find a valid root before or at version {}, maybe it was pruned?",
                    self.target_version
                ));

        Self::truncate_state_merkle_db(&state_merkle_db, state_merkle_target_version)?;

        let ledger_db = Arc::new(ledger_db);
        Self::truncate_ledger_db(
            Arc::clone(&ledger_db),
            ledger_db_version,
            self.target_version,
            self.ledger_db_batch_size,
        )?;

        if state_merkle_target_version < self.target_version {
            StateStore::catch_up_state_merkle_db(Arc::clone(&ledger_db), state_merkle_db)?;
        }

        Ok(())
    }

    fn get_current_version_in_ledger_db(ledger_db: &DB) -> Result<Option<Version>> {
        let mut iter = ledger_db.iter::<TransactionInfoSchema>(ReadOptions::default())?;
        iter.seek_to_last();
        Ok(iter.next().transpose()?.map(|item| item.0))
    }

    fn get_current_version_in_state_merkle_db(state_merkle_db: &DB) -> Result<Option<Version>> {
        Self::find_closest_node_version_at_or_before(state_merkle_db, u64::max_value())
    }

    fn find_tree_root_at_or_before(
        ledger_db: &DB,
        state_merkle_db: &DB,
        version: Version,
    ) -> Result<Option<Version>> {
        match Self::find_closest_node_version_at_or_before(state_merkle_db, version)? {
            Some(closest_version) => {
                if Self::root_exist_at_version(state_merkle_db, closest_version)? {
                    return Ok(Some(closest_version));
                }
                let mut iter = ledger_db.iter::<EpochByVersionSchema>(ReadOptions::default())?;
                iter.seek_for_prev(&version)?;
                match iter.next().transpose()? {
                    Some((closest_epoch_version, _)) => {
                        if Self::root_exist_at_version(state_merkle_db, closest_epoch_version)? {
                            Ok(Some(closest_epoch_version))
                        } else {
                            Ok(None)
                        }
                    },
                    None => Ok(None),
                }
            },
            None => Ok(None),
        }
    }

    fn root_exist_at_version(state_merkle_db: &DB, version: Version) -> Result<bool> {
        Ok(state_merkle_db
            .get::<JellyfishMerkleNodeSchema>(&NodeKey::new_empty_path(version))?
            .is_some())
    }

    fn find_closest_node_version_at_or_before(
        state_merkle_db: &DB,
        version: Version,
    ) -> Result<Option<Version>> {
        let mut iter = state_merkle_db.rev_iter::<JellyfishMerkleNodeSchema>(Default::default())?;
        iter.seek_for_prev(&NodeKey::new_empty_path(version))?;
        Ok(iter.next().transpose()?.map(|item| item.0.version()))
    }

    fn truncate_ledger_db(
        ledger_db: Arc<DB>,
        current_version: Version,
        target_version: Version,
        batch_size: usize,
    ) -> Result<()> {
        let event_store = EventStore::new(Arc::clone(&ledger_db));
        let transaction_store = TransactionStore::new(Arc::clone(&ledger_db));

        let mut current_version = current_version;
        while current_version > target_version {
            let start_version =
                std::cmp::max(current_version - batch_size as u64 + 1, target_version + 1);
            let end_version = current_version + 1;
            Self::truncate_ledger_db_single_batch(
                &ledger_db,
                &event_store,
                &transaction_store,
                start_version,
                end_version,
            )?;
            current_version = start_version - 1;
        }
        assert!(current_version == target_version);
        Ok(())
    }

    fn num_frozen_nodes_in_accumulator(num_leaves: u64) -> u64 {
        2 * num_leaves - num_leaves.count_ones() as u64
    }

    fn truncate_transaction_accumulator(
        ledger_db: &DB,
        start_version: Version,
        end_version: Version,
        batch: &SchemaBatch,
    ) -> Result<()> {
        let num_frozen_nodes = Self::num_frozen_nodes_in_accumulator(end_version);
        let mut iter = ledger_db.iter::<TransactionAccumulatorSchema>(ReadOptions::default())?;
        iter.seek_to_last();
        let (position, _) = iter.next().transpose()?.unwrap();
        assert!(position.to_postorder_index() + 1 == num_frozen_nodes);

        let num_frozen_nodes_after_this_batch =
            Self::num_frozen_nodes_in_accumulator(start_version);

        let mut num_nodes_to_delete = num_frozen_nodes - num_frozen_nodes_after_this_batch;

        let start_position = Position::from_postorder_index(num_frozen_nodes_after_this_batch);
        iter.seek(&start_position);

        assert!(start_position);

        for item in iter {
            let (position, _) = item;
            batch.delete::<TransactionAccumulatorSchema>(&position)?;
            num_nodes_to_delete -= 1;
        }

        assert!(num_nodes_to_delete == 0);

        Ok(())
    }

    fn truncate_ledger_db_single_batch(
        ledger_db: &DB,
        event_store: &EventStore,
        transaction_store: &TransactionStore,
        start_version: Version,
        end_version: Version,
    ) -> Result<()> {
        let batch = SchemaBatch::new();

        Self::delete_transaction_index_data(transaction_store, start_version, end_version, &batch)?;
        Self::delete_per_epoch_data(ledger_db, start_version, end_version, &batch)?;
        Self::delete_per_version_data(start_version, end_version, &batch)?;
        Self::delete_state_value_and_index(ledger_db, start_version, end_version, &batch)?;

        event_store.prune_events(start_version, end_version, &batch)?;

        Self::truncate_transaction_accumulator(ledger_db, start_version, end_version, &batch)?;

        ledger_db.write_schemas(batch)
    }

    fn delete_transaction_index_data(
        transaction_store: &TransactionStore,
        start_version: Version,
        end_version: Version,
        batch: &SchemaBatch,
    ) -> Result<()> {
        let transactions = transaction_store
            .get_transaction_iter(start_version, (end_version - start_version) as usize)?
            .collect::<Result<Vec<_>>>()?;
        transaction_store.prune_transaction_by_account(&transactions, batch)?;
        transaction_store.prune_transaction_by_hash(&transactions, batch)?;

        Ok(())
    }

    fn delete_per_epoch_data(
        ledger_db: &DB,
        start_version: Version,
        end_version: Version,
        batch: &SchemaBatch,
    ) -> Result<()> {
        let mut iter = ledger_db.iter::<EpochByVersionSchema>(ReadOptions::default())?;
        iter.seek(&start_version)?;

        for item in iter {
            let (version, epoch) = item?;
            assert!(version < end_version);
            batch.delete::<EpochByVersionSchema>(&version)?;
            batch.delete::<LedgerInfoSchema>(&epoch)?;
        }

        Ok(())
    }

    fn delete_per_version_data(
        start_version: Version,
        end_version: Version,
        batch: &SchemaBatch,
    ) -> Result<()> {
        for version in start_version..end_version {
            batch.delete::<TransactionInfoSchema>(&version)?;
            batch.delete::<TransactionSchema>(&version)?;
            batch.delete::<VersionDataSchema>(&version)?;
            batch.delete::<WriteSetSchema>(&version)?;
        }

        Ok(())
    }

    fn delete_state_value_and_index(
        ledger_db: &DB,
        start_version: Version,
        end_version: Version,
        batch: &SchemaBatch,
    ) -> Result<()> {
        let mut iter = ledger_db.iter::<StaleStateValueIndexSchema>(ReadOptions::default())?;
        iter.seek(&start_version)?;

        for item in iter {
            let (index, _) = item?;
            assert!(index.stale_since_version < end_version);
            batch.delete::<StaleStateValueIndexSchema>(&index)?;
            batch.delete::<StateValueSchema>(&(index.state_key, index.stale_since_version))?;
        }

        Ok(())
    }

    fn truncate_state_merkle_db(state_merkle_db: &DB, target_version: Version) -> Result<()> {
        loop {
            let batch = SchemaBatch::new();
            let current_version = Self::get_current_version_in_state_merkle_db(&state_merkle_db)?
                .expect("Current version of state merkle db must exist.");
            assert!(current_version >= target_version);
            if current_version == target_version {
                break;
            }

            let mut iter =
                state_merkle_db.iter::<JellyfishMerkleNodeSchema>(ReadOptions::default())?;
            iter.seek(&NodeKey::new_empty_path(current_version))?;
            for item in iter {
                let (key, _) = item?;
                batch.delete::<JellyfishMerkleNodeSchema>(&key)?;
            }

            Self::delete_stale_node_index_at_version::<StaleNodeIndexSchema>(
                state_merkle_db,
                current_version,
                &batch,
            )?;
            Self::delete_stale_node_index_at_version::<StaleNodeIndexCrossEpochSchema>(
                state_merkle_db,
                current_version,
                &batch,
            )?;

            state_merkle_db.write_schemas(batch)?;
        }

        Ok(())
    }

    fn delete_stale_node_index_at_version<S>(
        state_merkle_db: &DB,
        version: Version,
        batch: &SchemaBatch,
    ) -> Result<()>
    where
        S: Schema<Key = StaleNodeIndex>,
        Version: SeekKeyCodec<S>,
    {
        let mut iter = state_merkle_db.iter::<S>(ReadOptions::default())?;
        iter.seek(&version)?;
        for item in iter {
            let (index, _) = item?;
            assert!(index.stale_since_version == version);
            batch.delete::<S>(&index)?;
        }

        Ok(())
    }
}
