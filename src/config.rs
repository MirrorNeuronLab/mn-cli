use clap::{Arg, ArgAction, ArgMatches, Command, builder::ValueParser};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
pub struct Group {
    name: String,
    about: String,
    #[serde(default)]
    args: Vec<Parameter>,
    #[serde(default)]
    commands: Vec<Group>,
}
#[derive(Deserialize)]
struct Parameter {
    name: String,
    flags: Vec<String>,
    kind: String,
    help: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    multiple: bool,
    #[serde(default)]
    default: Value,
    #[serde(default)]
    choices: Vec<String>,
    min: Option<f64>,
    max: Option<f64>,
    #[serde(default)]
    secondary_opts: Vec<String>,
}
impl Parameter {
    fn parse(&self, s: &str) -> Result<Value, String> {
        if !self.choices.is_empty() && !self.choices.iter().any(|v| v == s) {
            return Err(format!("expected one of {}", self.choices.join(", ")));
        }
        let v = if self.kind.starts_with("int") {
            json!(s.parse::<i64>().map_err(|_| "expected an integer")?)
        } else if self.kind.starts_with("float") {
            json!(
                s.parse::<f64>()
                    .ok()
                    .filter(|n| n.is_finite())
                    .ok_or("expected a finite number")?
            )
        } else {
            json!(s)
        };
        if let Some(n) = v.as_f64()
            && (self.min.is_some_and(|m| n < m) || self.max.is_some_and(|m| n > m))
        {
            return Err("value is out of range".into());
        }
        Ok(v)
    }
    fn add(&self, mut cmd: Command) -> Command {
        let mut arg = Arg::new(self.name.clone())
            .required(self.required)
            .help(self.help.clone().unwrap_or_default());
        let mut long = false;
        for flag in self
            .flags
            .iter()
            .filter(|f| !self.secondary_opts.contains(f))
        {
            if let Some(s) = flag.strip_prefix("--") {
                arg = if long {
                    arg.visible_alias(s.to_owned())
                } else {
                    long = true;
                    arg.long(s.to_owned())
                };
            } else if let Some(s) = flag.strip_prefix('-') {
                arg = arg.short(s.chars().next().unwrap());
            }
        }
        if self.kind == "boolean" {
            arg = arg.action(ArgAction::SetTrue);
        } else {
            arg = arg.value_parser(ValueParser::string());
            if self.multiple {
                arg = arg.action(ArgAction::Append);
            }
        }
        for negative in &self.secondary_opts {
            let name = format!("neg-{}", self.name);
            cmd = cmd.arg(
                Arg::new(name)
                    .long(negative.trim_start_matches('-').to_owned())
                    .action(ArgAction::SetTrue)
                    .overrides_with(self.name.clone()),
            );
        }
        cmd.arg(arg)
    }
}
impl Group {
    pub fn root() -> Self {
        serde_json::from_str(include_str!("commands.json")).expect("invalid command contract")
    }
    pub fn build(&self) -> Command {
        let mut cmd = Command::new(self.name.clone()).about(self.about.clone());
        if self.name == "mn" {
            cmd = cmd
                .disable_version_flag(true)
                .arg(
                    Arg::new("version")
                        .short('v')
                        .visible_short_alias('V')
                        .long("version")
                        .action(ArgAction::Version),
                )
                .version(env!("CARGO_PKG_VERSION"));
        }
        for p in &self.args {
            cmd = p.add(cmd);
        }
        for c in &self.commands {
            cmd = cmd.subcommand(c.build());
        }
        cmd
    }
    pub fn values(
        &self,
        m: &ArgMatches,
        path: &mut Vec<String>,
        args: &mut Value,
    ) -> Result<(), String> {
        for p in &self.args {
            let value = if p.kind == "boolean" {
                json!(
                    if !p.secondary_opts.is_empty() && m.get_flag(&format!("neg-{}", p.name)) {
                        false
                    } else {
                        m.get_flag(&p.name) || p.default == true
                    }
                )
            } else if p.multiple {
                Value::Array(
                    m.get_many::<String>(&p.name)
                        .into_iter()
                        .flatten()
                        .map(|s| p.parse(s))
                        .collect::<Result<_, _>>()?,
                )
            } else if let Some(s) = m.get_one::<String>(&p.name) {
                p.parse(s)?
            } else {
                p.default.clone()
            };
            if !value.is_null() || args.get(&p.name).is_none() {
                args[&p.name] = value;
            }
        }
        if let Some((name, sub)) = m.subcommand() {
            path.push(name.to_owned());
            self.commands
                .iter()
                .find(|c| c.name == name)
                .unwrap()
                .values(sub, path, args)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_command_tree_and_option_aliases_are_consistent() {
        let root = Group::root();
        root.build().debug_assert();
        let matches = root
            .build()
            .try_get_matches_from([
                "mn",
                "blueprint",
                "run",
                "demo",
                "--node",
                "worker",
                "--queue",
                "--detached",
                "--json",
            ])
            .unwrap();
        let mut path = Vec::new();
        let mut options = json!({});
        root.values(&matches, &mut path, &mut options).unwrap();
        assert_eq!(path, ["blueprint", "run"]);
        assert_eq!(options["owner_node"], "worker");
        assert_eq!(options["queue"], true);
        assert_eq!(options["json_output"], true);
    }
}
