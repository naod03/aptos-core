module post_mint_reveal_nft::whitelist {
    use post_mint_reveal_nft::bucket_table::BucketTable;
    use std::vector;
    use std::error;
    use post_mint_reveal_nft::bucket_table;
    use std::signer;
    use aptos_framework::timestamp;
    use aptos_framework::account;
    friend post_mint_reveal_nft::minting;

    /// WhitelistMintConfig stores information about all stages of whitelist.
    /// Most whitelists are one-stage, but we allow multiple stages to be added in case there are multiple rounds of whitelists.
    struct WhitelistMintConfig has key {
        whitelist_configs: vector<WhitelistStage>,
    }

    /// WhitelistMintConfigSingleStage stores information about one stage of whitelist.
    struct WhitelistStage has store {
        whitelisted_address: BucketTable<address, u64>,
        whitelist_mint_price: u64,
        whitelist_minting_start_time: u64,
        whitelist_minting_end_time: u64,
    }

    const EINVALID_WHITELIST_SETTING: u64 = 1;
    const EINVALID_STAGE: u64 = 2;
    const EACCOUNT_DOES_NOT_EXIST: u64 = 3;
    const EEXCEEDS_MINT_LIMIT: u64 = 4;
    const EINVALID_UPDATE_AFTER_MINTING: u64 = 5;


    public(friend) fun whitelist_config_exists(admin: &signer): bool {
        exists<WhitelistMintConfig>(signer::address_of(admin))
    }

    public(friend) fun is_whitelist_minting_time(module_address: address): bool acquires WhitelistMintConfig {
        let whitelist_mint_config = borrow_global_mut<WhitelistMintConfig>(module_address);
        let first_stage_start_time = vector::borrow(&whitelist_mint_config.whitelist_configs, 0).whitelist_minting_start_time;
        let last_stage_end_time = vector::borrow(&whitelist_mint_config.whitelist_configs, vector::length(&whitelist_mint_config.whitelist_configs) - 1).whitelist_minting_end_time;
        let now = timestamp::now_seconds();
        now >= first_stage_start_time && now < last_stage_end_time
    }

    public(friend) fun get_num_of_stages(module_address: address): u64 acquires WhitelistMintConfig {
        vector::length(&borrow_global<WhitelistMintConfig>(module_address).whitelist_configs)
    }

    public(friend) fun init_whitelist_config(admin: &signer) {
        let config = WhitelistMintConfig {
            whitelist_configs: vector::empty<WhitelistStage>(),
        };
        move_to(admin, config);
    }

    public(friend) fun add_whitelist_stage(admin: &signer, whitelist_start_time: u64, whitelist_end_time: u64, whitelist_price: u64, stage: u64) acquires WhitelistMintConfig {
        assert!(whitelist_start_time < whitelist_end_time, error::invalid_argument(EINVALID_WHITELIST_SETTING));
        let config = borrow_global_mut<WhitelistMintConfig>(signer::address_of(admin));
        assert!(vector::length(&config.whitelist_configs) == stage, error::invalid_argument(EINVALID_STAGE));
        let whitelist_stage = WhitelistStage {
            whitelisted_address: bucket_table::new<address, u64>(4),
            whitelist_mint_price: whitelist_price,
            whitelist_minting_start_time: whitelist_start_time,
            whitelist_minting_end_time: whitelist_end_time,
        };
        vector::push_back(&mut config.whitelist_configs, whitelist_stage);
    }

    public(friend) fun add_whitelist_addresses(admin: &signer, wl_addresses: vector<address>, mint_limit: u64, stage: u64) acquires WhitelistMintConfig {
        let config = borrow_global_mut<WhitelistMintConfig>(signer::address_of(admin));
        assert!(stage < vector::length(&config.whitelist_configs), error::invalid_argument(EINVALID_STAGE));
        let whitelist_stage = vector::borrow_mut(&mut config.whitelist_configs, stage);
        let now = timestamp::now_seconds();
        assert!(now < whitelist_stage.whitelist_minting_end_time, error::invalid_argument(EINVALID_UPDATE_AFTER_MINTING));

        let i = 0;
        while (i < vector::length(&wl_addresses)) {
            let addr = *vector::borrow(&wl_addresses, i);
            // assert that the specified address exists
            assert!(account::exists_at(addr), error::invalid_argument(EACCOUNT_DOES_NOT_EXIST));
            bucket_table::add(&mut whitelist_stage.whitelisted_address, addr, mint_limit);
            i = i + 1;
        };
    }

    /// WhitelistMintConfigSingleStage stores information about one stage of whitelist.
    public(friend) fun check_if_user_can_mint(module_address: address, minter_address: address, stage: u64, amount: u64): (u64, bool, bool) acquires WhitelistMintConfig {
        let whitelist_mint_config = borrow_global_mut<WhitelistMintConfig>(module_address);
        assert!(stage < vector::length(&whitelist_mint_config.whitelist_configs), error::invalid_argument(EINVALID_STAGE));
        let config = vector::borrow_mut(&mut whitelist_mint_config.whitelist_configs, stage);
        let now = timestamp::now_seconds();
        let user_address_on_whitelist = bucket_table::contains(&config.whitelisted_address, &minter_address);
        let is_whitelist_minting_time = now >= config.whitelist_minting_start_time && now < config.whitelist_minting_end_time;

        if (user_address_on_whitelist && is_whitelist_minting_time) {
            let remaining_amount = bucket_table::borrow_mut(&mut config.whitelisted_address, minter_address);
            assert!(amount <= *remaining_amount, error::invalid_argument(EEXCEEDS_MINT_LIMIT));
            *remaining_amount = *remaining_amount - amount;
        };

        (config.whitelist_mint_price, user_address_on_whitelist, is_whitelist_minting_time)
    }
}
