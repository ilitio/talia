use anyhow::Result;
use talia_agent::runner;

pub(crate) fn execute() -> Result<()> {
    for spec in runner::collector_specs() {
        println!("{}", spec.name);
    }
    Ok(())
}
