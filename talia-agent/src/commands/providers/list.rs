use anyhow::Result;
use talia_agent::runner;

pub(crate) fn execute(descriptions: bool) -> Result<()> {
    for spec in runner::collector_specs() {
        if descriptions {
            println!("{}: {}", spec.name, spec.brief_description);
        } else {
            println!("{}", spec.name);
        }
    }
    Ok(())
}
