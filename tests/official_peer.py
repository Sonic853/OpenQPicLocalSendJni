"""Build a fixture against an unmodified LocalSend core checkout, outside that checkout.

python tests/official_peer.py /path/to/localsend
build/official/target/debug/examples/openqpic_peer[.exe] send FORWARDED_PORT
build/official/target/debug/examples/openqpic_peer[.exe] receive OUTPUT_DIR
"""
from pathlib import Path
import shutil
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
upstream = Path(sys.argv[1]).resolve()
out = root/"build/official"
shutil.copytree(upstream/"packages/core", out, dirs_exist_ok=True)
shutil.copyfile(upstream/"Cargo.lock", out/"Cargo.lock")
# Reuse upstream's real client, server, streaming body and filesystem test helpers.
helpers = (out/"tests/v2_server.rs").read_text("utf-8").split("#[tokio::test]")[0]
# Upstream's unit-test fixture runs with trusted in-process test inputs. Restrict
# our external smoke fixture to the emulator's loopback host connection and size.
assert "files, decision_tx, .." in helpers and "let path = dir.join(&file_id);" in helpers
helpers = helpers.replace("files, decision_tx, ..", "ip, files, decision_tx, ..").replace(
    "match accept {", "match accept && ip.ip.is_loopback() && files.len() == 1 && "
    "files.values().all(|f| f.size == 262912) {").replace(
    "let path = dir.join(&file_id);", 'let path = dir.join("fixture.bin");')
main = r'''
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args[1] == "receive" {
        let dir=PathBuf::from(&args[2]); std::fs::create_dir_all(&dir).unwrap();
        let server=start_test_server(None,true,Some(dir)).await;
        println!("PORT={}",server.port);
        tokio::time::timeout(Duration::from_secs(180),async {
            while server.received.lock().await.is_empty() { tokio::time::sleep(Duration::from_millis(100)).await; }
        }).await.unwrap();
        println!("RECEIVED={:?}",server.received.lock().await.keys()); return;
    }
    let port=args[2].parse().unwrap();
    let client=LsHttpClientV2::try_new_without_cert().unwrap();
    client.register(ProtocolType::Http,"127.0.0.1",port,sender_info()).await.unwrap();
    let data:Vec<u8>=(0..100000u32).map(|n|n as u8).collect();
    let mut a=file_dto("a","官方-📷.bin",data.len() as u64); a.sha256=Some(sha256_hex(&data));
    let b=file_dto("b","empty.bin",0);
    let r=client.prepare_upload(ProtocolType::Http,"127.0.0.1",port,None,
        prepare_upload_request(&[a,b]),None,CancellationToken::new()).await.unwrap().response.unwrap();
    tokio::try_join!(upload_bytes(&client,port,&r.session_id,"a",&r.files["a"],&data),
                     upload_bytes(&client,port,&r.session_id,"b",&r.files["b"],&[])).unwrap();
    println!("SENT=100000+0 SHA256={}",sha256_hex(&data));
}
'''
(out/"examples/openqpic_peer.rs").write_text(helpers + main, "utf-8")
subprocess.run(["cargo", "build", "-j2", "--features", "http", "--example", "openqpic_peer"], cwd=out, check=True)
