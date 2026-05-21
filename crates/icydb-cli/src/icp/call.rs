use std::process::Command;

pub(crate) fn icp_query_command(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg(candid_arg)
        .arg("--query")
        .arg("--output")
        .arg("hex")
        .arg("--environment")
        .arg(environment);

    command
}

pub(crate) fn icp_update_command(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg(candid_arg)
        .arg("--output")
        .arg("hex")
        .arg("--environment")
        .arg(environment);

    command
}

pub(crate) fn hex_response_bytes(output: &str) -> Result<Vec<u8>, String> {
    let candidate = output
        .rsplit_once("response (hex):")
        .map_or(output, |(_, value)| value)
        .trim();
    let hex = candidate.split_whitespace().collect::<String>();
    if hex.is_empty() {
        return Err("icp canister call returned an empty hex response".to_string());
    }
    if hex.len() % 2 != 0 {
        return Err("icp canister call returned odd-length hex response".to_string());
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }

    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        other => Err(format!(
            "icp canister call returned non-hex byte '{}'",
            char::from(other)
        )),
    }
}
