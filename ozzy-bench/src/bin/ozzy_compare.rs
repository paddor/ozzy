#![forbid(unsafe_code)]
use clap::Parser;
use ozzy_bench::automation::{
    Result,
    compare::{Args, run},
};
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    run(Args::parse()).await
}
