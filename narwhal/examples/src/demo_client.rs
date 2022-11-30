// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::{crate_name, crate_version, App, AppSettings, Arg, SubCommand};
use narwhal::{
    collection_retrieval_result::RetrievalResult, proposer_client::ProposerClient,
    validator_client::ValidatorClient, CertificateDigest, CollectionRetrievalResult, Empty,
    GetCollectionsRequest, GetCollectionsResponse, NodeReadCausalRequest, NodeReadCausalResponse,
    PublicKey, ReadCausalRequest, ReadCausalResponse, RemoveCollectionsRequest, RoundsRequest,
    RoundsResponse, Transaction,
};

use futures::StreamExt;
use prost::bytes::Bytes;
use std::{
    fmt,
    fmt::{Display, Formatter},
};
use tonic::{Status, transport::Channel};
use types::{TransactionProto, TransactionsClient};

pub mod narwhal {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("narwhal");
}
use node::blockchain::{Block, ExecutionError, Transaction as ChainTx};
use std::{thread, time::Duration};

// Assumption that each transaction costs 1 gas to complete
// Chose this number because it allows demo to complete round + get extra collections when proposing block.
const BLOCK_GAS_LIMIT: u32 = 200_000;
// const ROUNDS_PER_BLOCK: u64 = 2;
const RE_ADD_TXS: bool = false;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A gRPC client emulating the Proposer / Validator API")
        .subcommand(
            SubCommand::with_name("run")
                .about("Run the demo with a local gRPC server")
                .arg(
                    Arg::with_name("keys")
                        .long("keys")
                        .help("The base64-encoded publickey of the node to query")
                        .use_delimiter(true)
                        .min_values(2),
                )
                .arg(
                    Arg::with_name("ports")
                        .long("ports")
                        .help("The ports on localhost where to reach the grpc server")
                        .use_delimiter(true)
                        .min_values(2),
                )
                .arg(
                    Arg::with_name("client-index")
                        .long("client-index")
                        .help("The client number")
                        .min_values(1),
                )
                .arg(
                    Arg::with_name("blocks")
                        .long("blocks")
                        .help("Run until reaching this amount of blocks")
                        .min_values(1),
                )
                .arg(
                    Arg::with_name("rounds-per-block")
                        .long("rounds-per-block")
                        .help("Narwhal rounds to create a block")
                        .min_values(1),
                ),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let mut dsts = Vec::new();
    let mut base64_keys = Vec::new();
    let mut client: usize = 0;
    let mut blocks_to_run: u64 = 1;
    let mut rounds_per_block: u64 = 1;
    match matches.subcommand() {
        ("run", Some(sub_matches)) => {
            let ports = sub_matches
                .values_of("ports")
                .expect("Invalid ports specified");
            // TODO : check this arg is correctly formatted (number < 65536)
            for port in ports {
                dsts.push(format!("http://127.0.0.1:{port}"))
            }
            let keys = sub_matches
                .values_of("keys")
                .expect("Invalid public keys specified");
            // TODO : check this arg is correctly formatted (pk in base64)
            for key in keys {
                base64_keys.push(key.to_owned())
            }
            let client_aux = sub_matches
                .value_of("client-index")
                .expect("Invalid client number specified");
            client = client_aux.parse::<i32>().unwrap() as usize;
            let blocks_aux = sub_matches
                .value_of("blocks")
                .expect("Invalid blocks specified");
            blocks_to_run = blocks_aux.parse::<u64>().unwrap();
            let rounds_per_block_aux = sub_matches
                .value_of("rounds-per-block")
                .expect("Invalid rounds per block specified");
            rounds_per_block = rounds_per_block_aux.parse::<u64>().unwrap();
        }
        _ => unreachable!(),
    }
    println!("Client {}!", client);
    println!("Blocks to run {}!", blocks_to_run);
    println!("Rounds per block {}!", rounds_per_block);

    let mut current_block = Block::genesis(BLOCK_GAS_LIMIT as u32).next();
    let narwhal_nodes = base64_keys.len() as u64;

    println!(
        "******************************** Proposer Service ********************************\n"
    );
    println!("\nConnecting to {} as the proposer.", dsts[client]);
    let mut proposer_client = ProposerClient::connect(dsts[client].clone()).await?;
    let mut validator_client = ValidatorClient::connect(dsts[client].clone()).await?;
    // let public_key = base64::decode(&base64_keys[0]).unwrap();
    // let public_key = get_proposer_for_block(0, base64_keys);

    println!("\n1) Retrieve the range of rounds you have a collection for");
    println!("\n\t---- Use Rounds endpoint ----\n");

    // // Q: Why is this for a specific validator?
    // let rounds_request = RoundsRequest {
    //     public_key: Some(PublicKey {
    //         bytes: get_proposer_for_block(0, base64_keys.clone(), narwhal_nodes as u64).clone(),
    //     }),
    // };

    // println!("\t{}\n", rounds_request);

    // let request = tonic::Request::new(rounds_request);
    // let response = proposer_client.rounds(request).await;
    // let rounds_response = response.unwrap().into_inner();

    // println!("\t{}\n", rounds_response);

    // let oldest_round = rounds_response.oldest_round;
    // let newest_round = rounds_response.newest_round;
    // let mut round = oldest_round + 1;
    let mut round = 0;
    // let mut last_completed_round = round;

    println!("\n2) Find collections from earliest round and continue to add collections until gas limit is hit\n");
    let mut block_proposal_collection_ids = Vec::new();
    // let mut extra_collections = Vec::new();
    while round <= (blocks_to_run * rounds_per_block) {
        let mut max_round;
        loop {
            max_round = get_max_round(proposer_client.clone(), current_block.number, base64_keys.clone(), narwhal_nodes).await;
            // println!("Max round: {}", max_round);
            if max_round > (current_block.number * rounds_per_block) {
                break;
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
        let proposer_public_key =
           get_proposer_for_block(round / rounds_per_block, base64_keys.clone(), narwhal_nodes);
        // NOTE: Uncomment to have every client getting their collections for each block
        // let proposer_public_key = base64::decode(&base64_keys[client]).unwrap();
        let mut block_full = false;
        let mut failed_txs = Vec::new();
        let mut gas_overload_txs = Vec::new();

        let node_read_causal_request = NodeReadCausalRequest {
            public_key: Some(PublicKey {
                bytes: proposer_public_key,
            }),
            round,
        };

        println!("\t-------------------------------------");
        println!("\t| 2a) Find collections for round = {}", round);
        println!("\t-------------------------------------");

        println!("\t{}\n", node_read_causal_request);

        let request = tonic::Request::new(node_read_causal_request);
        let response = proposer_client.node_read_causal(request).await;

        if let Some(node_read_causal_response) = println_and_into_inner(response) {
            let mut duplicate_collection_count = 0;
            let mut new_collections = Vec::new();
            let count_of_retrieved_collections = node_read_causal_response.collection_ids.len();
            for collection_id in node_read_causal_response.collection_ids {
                if block_proposal_collection_ids.contains(&collection_id) {
                    duplicate_collection_count += 1;
                } else {
                    println!(
                        "\n\t\t2b) Get collection [{}] payloads to calculate gas cost of proposed block.\n", collection_id
                    );

                    let get_collections_request = GetCollectionsRequest {
                        collection_ids: vec![collection_id.clone()],
                    };

                    println!("\t\t{}\n", get_collections_request);

                    let request = tonic::Request::new(get_collections_request);
                    let response = validator_client.get_collections(request).await;
                    let get_collection_response = response.unwrap().into_inner();

                    let (total_num_of_transactions, total_transactions_size, txs) =
                        get_total_transaction_count_and_size(
                            get_collection_response.result.clone(),
                        );

                    let decoded_txs = txs
                        .into_iter()
                        .map(|tx| {
                            let mut data_bytes = Bytes::copy_from_slice(tx.transaction.as_slice());
                            ChainTx::deserialize(&mut data_bytes)
                        })
                        .enumerate()
                        // .inspect(|(i, tx)| {
                        //     println!("\t\t\tDeserialized tx {i}: {tx:?}");
                        // })
                        .map(|(_, tx)| tx);

                    // Store state for rollback in case of reaching gas limit
                    let start_block = current_block.clone();
                    for tx in decoded_txs {
                        if block_full {
                            gas_overload_txs.push(tx);
                        } else {
                            match current_block.try_apply_tx(&tx) {
                                Err(ExecutionError::GasLimitReached) => {
                                    block_full = true;
                                    gas_overload_txs.push(tx);
                                }
                                Err(ExecutionError::InvalidTransaction) => {
                                    failed_txs.push(tx);
                                }
                                _ => {}
                            }
                        }
                    }

                    println!("\t\tFound {total_num_of_transactions} transactions with a total size of {total_transactions_size} bytes");

                    new_collections.push(collection_id);
                }
            }
            println!(
                "\t\t {} collections were used in the block proposal from round {round}",
                new_collections.len()
            );

            block_proposal_collection_ids.extend(new_collections.clone());

            println!("\t\tDeduped {:?} collections\n", duplicate_collection_count);

            // NOTE: uncomment to prune only the collections of the client zero
            // if client == 0 {
            println!("\n\t\t2c) Remove collections that have been used for the block.\n");

            let remove_collections_request = RemoveCollectionsRequest {
                collection_ids: new_collections.clone(),
            };

            println!("\t{}\n", remove_collections_request);

            let request = tonic::Request::new(remove_collections_request);
            let response = validator_client.remove_collections(request).await;
            if response.is_ok() {
                println!("\tSuccessfully removed committed collections\n");
            } else {
                println!("\tWas not able to remove committed collections\n");
            }
            //}
        } else {
            println!("\tError trying to node read causal at round {round}\n")
        }

        // Re-add AND just the proposer
        if RE_ADD_TXS && round / rounds_per_block % narwhal_nodes == (client as u64) {
            println!("\n2b2) Adding back failed transactions back to narwhal.\n");
            println!("---- Use TransactionClient.SubmitTransactionStream endpoint ----\n");
            // Connect to the mempool.
            let mut client = TransactionsClient::connect(dsts[client].clone())
                .await
                .expect("Could not create TransactionsClient");
            let stream = tokio_stream::iter(
                [failed_txs.clone(), gas_overload_txs.clone()].concat(),
            )
            .map(move |tx| {
                println!("Resending tx {:?}", &tx);
                TransactionProto {
                    transaction: tx.serialize(),
                }
            });

            if let Err(e) = client.submit_transaction_stream(stream).await {
                println!("Failed to send transaction: {e}");
                // FIXME: Not sure why this keeps happening, ignore for now
                // return Ok(());
            }
        }

        println!(
            "\t\tThere were {} transactions which failed to execute",
            failed_txs.len()
        );
        println!(
            "\t\tThere were {} transactions which were not able to be part of the block",
            gas_overload_txs.len()
        );
        if RE_ADD_TXS {
            println!("\t\tAdding them back to narwhal");
        }

        println!("\t\t=====================================================================");
        println!(
            "\t\tFinalized block {}\n\t\t\twith state hash {:x},\n\t\t\tgas limit {},\n\t\t\tgas used {},\n\t\t\t# txs {}, \n\t\t\tlast hash {:x}",
            current_block.number,
            current_block.root(),
            current_block.gas_limit,
            current_block.gas_used,
            current_block.transactions.len(),
            current_block.last_hash
        );
        println!("\t\t=====================================================================");
        current_block = current_block.next();
        round += rounds_per_block;
    }
    println!("\n\tEverything it's ok babe!\n");
    Ok(())
}

async fn get_max_round(proposer_client: ProposerClient<Channel>, block_number: u64, base64_keys: Vec<String>, validators: u64) -> u64 {
    // Q: Why is this for a specific validator?
    let rounds_request = RoundsRequest {
        public_key: Some(PublicKey {
            bytes: get_proposer_for_block(block_number, base64_keys.clone(), validators).clone(),
        }),
    };

    // println!("\t{}\n", rounds_request);

    let request = tonic::Request::new(rounds_request);
    let response = proposer_client.clone().rounds(request).await;
    let rounds_response = response.unwrap().into_inner();

    // println!("\t{}\n", rounds_response.newest_round);
    return rounds_response.newest_round
}

fn get_proposer_for_block(block_number: u64, base64_keys: Vec<String>, validators: u64) -> Vec<u8> {
    return base64::decode(&base64_keys[(block_number % validators) as usize]).unwrap();
}

fn get_total_transaction_count_and_size(
    result: Vec<CollectionRetrievalResult>,
) -> (i32, usize, Vec<Transaction>) {
    let mut total_num_of_transactions = 0;
    let mut total_transactions_size = 0;
    let mut transactions = Vec::with_capacity(result.len());

    for r in result {
        match r.retrieval_result.unwrap() {
            RetrievalResult::Collection(collection) => {
                for t in collection.transactions {
                    total_transactions_size += t.transaction.len();
                    total_num_of_transactions += 1;
                    transactions.push(t)
                }
            }
            RetrievalResult::Error(_) => {}
        }
    }
    (
        total_num_of_transactions,
        total_transactions_size,
        transactions,
    )
}

////////////////////////////////////////////////////////////////////////
/// Formatting the requests and responses                             //
////////////////////////////////////////////////////////////////////////
impl Display for GetCollectionsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** GetCollectionsRequest ***".to_string();
        for id in &self.collection_ids {
            result = format!("{}\n\t\t|-id=\"{}\"", result, id);
        }
        write!(f, "{}", result)
    }
}

impl Display for RemoveCollectionsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** RemoveCollectionsRequest ***".to_string();
        for id in &self.collection_ids {
            result = format!("{}\n\t|-id=\"{}\"", result, id);
        }
        write!(f, "{}", result)
    }
}

impl Display for Empty {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let result = "*** Empty ***".to_string();
        write!(f, "{}", result)
    }
}

impl Display for GetCollectionsResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** GetCollectionsResponse ***".to_string();

        for r in self.result.clone() {
            match r.retrieval_result.unwrap() {
                RetrievalResult::Collection(collection) => {
                    let collection_id = &collection.id.unwrap();
                    let mut transactions_size = 0;
                    let mut num_of_transactions = 0;

                    for t in collection.transactions {
                        transactions_size += t.transaction.len();
                        num_of_transactions += 1;
                    }

                    result = format!(
                        "{}\n\t|-Collection id {}, transactions {}, size: {} bytes",
                        result, collection_id, num_of_transactions, transactions_size
                    );
                }
                RetrievalResult::Error(error) => {
                    result = format!(
                        "{}\n\tError for certificate id {}, error: {}",
                        result,
                        &error.id.unwrap(),
                        error.error
                    );
                }
            }
        }

        write!(f, "{}", result)
    }
}

impl Display for NodeReadCausalResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** NodeReadCausalResponse ***".to_string();

        for id in &self.collection_ids {
            result = format!("{}\n\t|-id=\"{}\"", result, id);
        }

        write!(f, "{}", result)
    }
}

impl Display for NodeReadCausalRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** NodeReadCausalRequest ***".to_string();

        result = format!("{}\n\t|-Request for round {}", result, &self.round);
        result = format!(
            "{}\n\t|-Authority: {}",
            result,
            base64::encode(&self.public_key.clone().unwrap().bytes)
        );

        write!(f, "{}", result)
    }
}

impl Display for ReadCausalResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** ReadCausalResponse ***".to_string();

        for id in &self.collection_ids {
            result = format!("{}\n\tid=\"{}\"", result, id);
        }

        write!(f, "{}", result)
    }
}

impl Display for ReadCausalRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** ReadCausalRequest ***".to_string();

        result = format!(
            "{}\n\t|-Request for collection {}",
            result,
            &self.collection_id.as_ref().unwrap()
        );

        write!(f, "{}", result)
    }
}

impl Display for RoundsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** RoundsRequest ***".to_string();

        result = format!(
            "{}\n\t|-Authority: {}",
            result,
            base64::encode(&self.public_key.clone().unwrap().bytes)
        );

        write!(f, "{}", result)
    }
}

impl Display for RoundsResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** RoundsResponse ***".to_string();
        result = format!(
            "{}\n\t|-oldest_round: {}, newest_round: XXX",
            result, &self.oldest_round//, &self.newest_round
        );

        write!(f, "{}", result)
    }
}

impl Display for CertificateDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", base64::encode(&self.digest))
    }
}

fn println_and_into_inner<T>(result: Result<tonic::Response<T>, Status>) -> Option<T>
where
    T: Display,
{
    match result {
        Ok(response) => {
            let inner = response.into_inner();
            println!("\t{}", &inner);
            Some(inner)
        }
        Err(error) => {
            println!("\t{:?}", error);
            None
        }
    }
}
