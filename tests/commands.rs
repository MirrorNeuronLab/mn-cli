use std::process::Command;

#[test]
fn version_aliases_print_the_installed_version() {
    for option in ["--version", "-v", "-V"] {
        let output = Command::new(env!("CARGO_BIN_EXE_mn"))
            .arg(option)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            concat!("mn ", env!("CARGO_PKG_VERSION"))
        );
    }
}

#[test]
fn every_command_in_the_public_catalog_has_an_executable_help_path() {
    fn visit(group: &serde_json::Value, path: &mut Vec<String>) {
        if let Some(children) = group["commands"].as_array() {
            for child in children {
                path.push(child["name"].as_str().unwrap().to_owned());
                visit(child, path);
                path.pop();
            }
        } else {
            let output = Command::new(env!("CARGO_BIN_EXE_mn"))
                .args(&*path)
                .arg("--help")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{path:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("Usage:"));
        }
    }
    visit(
        &serde_json::from_str(include_str!("../src/commands.json")).unwrap(),
        &mut vec![],
    );
}

#[test]
fn removed_commands_still_point_the_user_to_their_replacement() {
    let out = Command::new(env!("CARGO_BIN_EXE_mn"))
        .args(["run", "monitor", "--json"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let envelope: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(envelope["error"]["code"], "MN_VALIDATION_FAILED");
    assert!(
        envelope["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mn run watch")
    );
}
