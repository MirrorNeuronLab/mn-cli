use mn_client_support::Error;
pub(crate) fn unsupported(msg: &str) -> Error {
    Error::new(422, "MN_CLIENT_UNSUPPORTED", msg)
}
pub(crate) fn replacement(argv: &[std::ffi::OsString]) -> Option<&'static str> {
    let mut path = argv
        .iter()
        .skip(1)
        .filter_map(|s| s.to_str())
        .filter(|s| !s.starts_with('-'));
    Some(match (path.next()?, path.next()?) {
        ("blueprint", "install") => "mn blueprint add",
        ("blueprint", "uninstall") => "mn blueprint remove",
        ("blueprint", "monitor") => "mn run list --follow",
        ("blueprint", "tail") => "mn run logs RUN_ID --channel events",
        ("blueprint", "logs") => "mn run logs RUN_ID --channel logs",
        ("blueprint", "stream") => "mn run logs RUN_ID --channel all --follow",
        ("blueprint", "resources") => "mn run resources RUN_ID",
        ("blueprint", "compare") => "mn run compare RUN_A RUN_B",
        ("job", "inspect") => "mn job show",
        ("job", "runs") => "mn run list --job JOB_ID",
        ("run", "status") => "mn run show",
        ("run", "monitor") => "mn run watch",
        ("runtime", "health") => "mn runtime status (or mn runtime doctor for diagnostics)",
        ("runtime", "metrics") => "mn resource usage",
        ("runtime", "update") => "mn runtime upgrade",
        ("node", "join") => "mn node add HOST --token TOKEN",
        ("node", "expose") => "mn runtime start",
        ("node", "leave") => "mn node remove",
        ("operation", "status") => "mn operation show",
        ("resource", "list") => "mn resource show",
        ("resource", "ports") => "mn runtime status",
        ("service", "resolve") => "mn service show",
        ("service", "check") => "mn blueprint doctor",
        ("model", "install") => "mn model add",
        ("model", "proxy") => "mn model add --file DEFINITION.json",
        ("model", "remote") => "mn model add --file DEFINITION.json",
        _ => return None,
    })
}
