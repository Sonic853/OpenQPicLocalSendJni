"""Build a fixture against an unmodified LocalSend core checkout, outside that checkout.

python tests/official_peer.py /path/to/localsend
build/official/target/debug/examples/openqpic_peer[.exe] send FORWARDED_PORT
build/official/target/debug/examples/openqpic_peer[.exe] receive OUTPUT_DIR
"""
from pathlib import Path
import shutil
import subprocess
import sys
import io
import struct
import wave
import zlib

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
gate = r'''if let Ok(dir) = std::env::var("OPENQPIC_TEST_GATE") {
                            let dir=PathBuf::from(dir);
                            std::fs::write(dir.join("waiting"), b"prepare").unwrap();
                            tokio::time::timeout(Duration::from_secs(120),async {
                                while !dir.join("resume").exists() { tokio::time::sleep(Duration::from_millis(50)).await; }
                            }).await.unwrap();
                        }
                        '''
helpers = helpers.replace("files, decision_tx, ..", "ip, files, decision_tx, ..").replace(
    "match accept {", "match accept && ip.ip.is_loopback() && (1..=2).contains(&files.len()) && "
    "files.values().all(|f| f.size == 262912 || f.size == 196611) {").replace(
    "let path = dir.join(&file_id);", 'let path = dir.join(sha256_hex(file_id.as_bytes()));').replace(
    "let _ = decision_tx.send(decision);",gate+"let _ = decision_tx.send(decision);").replace(
    '"Test Server".to_string()', 'std::env::var("OPENQPIC_TEST_NAME").unwrap_or_else(|_| "Test Server".to_string())').replace(
    '"server-fingerprint".to_string()', 'std::env::var("OPENQPIC_TEST_NAME").unwrap_or_else(|_| "server-fingerprint".to_string())')
def chunk(tag,data):
    return struct.pack(">I",len(data))+tag+data+struct.pack(">I",zlib.crc32(tag+data))
png=b"\x89PNG\r\n\x1a\n"+chunk(b"IHDR",struct.pack(">2I5B",2,2,8,2,0,0,0))
png+=chunk(b"IDAT",zlib.compress((b"\0"+b"\x12\x34\x56"*2)*2))+chunk(b"IEND",b"")
(out/"examples/photo.png").write_bytes(png)
sound=io.BytesIO()
with wave.open(sound,"wb") as wav:
    wav.setparams((1,2,16000,0,"NONE","not compressed"))
    wav.writeframes(bytes(range(256))*391)
(out/"examples/sound.wav").write_bytes(sound.getvalue())
main = r'''
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args[1] == "receive" || args[1] == "receive-gated" {
        if args[1] == "receive-gated" { std::env::set_var("OPENQPIC_TEST_GATE",&args[3]); }
        if let Some(name)=args.get(4) { std::env::set_var("OPENQPIC_TEST_NAME",name); }
        let expected=args.get(5).map_or(1,|n| n.parse::<usize>().unwrap());
        assert!((1..=2).contains(&expected));
        let dir=PathBuf::from(&args[2]); std::fs::create_dir_all(&dir).unwrap();
        let server=start_test_server(None,!args.get(6).is_some_and(|s| s=="decline"),Some(dir)).await;
        println!("PORT={}",server.port);
        tokio::time::timeout(Duration::from_secs(180),async {
            while server.received.lock().await.len()<expected { tokio::time::sleep(Duration::from_millis(100)).await; }
        }).await.unwrap();
        println!("RECEIVED={:?}",server.received.lock().await.keys()); return;
    }
    let port=args[2].parse().unwrap();
    let client=LsHttpClientV2::try_new_without_cert().unwrap();
    client.register(ProtocolType::Http,"127.0.0.1",port,sender_info()).await.unwrap();
    let data=include_bytes!("photo.png"); let audio=include_bytes!("sound.wav");
    let mut a=file_dto("a","官方-📷.png",data.len() as u64); a.sha256=Some(sha256_hex(data));
    a.file_type="image/png".into();
    let b=file_dto("b","sound.wav",audio.len() as u64); // generic MIME, known media extension
    let doc=file_dto("doc","notes.txt",11);
    let r=client.prepare_upload(ProtocolType::Http,"127.0.0.1",port,None,
        prepare_upload_request(&[a,b,doc]),None,CancellationToken::new()).await.unwrap().response.unwrap();
    assert_eq!(r.files.len(),2); assert!(!r.files.contains_key("doc"));
    tokio::try_join!(upload_bytes(&client,port,&r.session_id,"a",&r.files["a"],data),
                     upload_bytes(&client,port,&r.session_id,"b",&r.files["b"],audio)).unwrap();
    println!("PNG={} WAV={}",sha256_hex(data),sha256_hex(audio));
}
'''
(out/"examples/openqpic_peer.rs").write_text(helpers + main, "utf-8")
subprocess.run(["cargo", "build", "-j2", "--features", "http", "--example", "openqpic_peer"], cwd=out, check=True)
