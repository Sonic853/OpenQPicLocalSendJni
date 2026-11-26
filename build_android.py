"""Build stripped Android transports; check machine, JNI exports and arm64 load alignment."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
ABIS = {"armeabi": "ARM", "x86": "Intel 80386", "arm64-v8a": "AArch64"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ndk", required=True, type=Path, help="NDK r16b for armeabi/x86")
    parser.add_argument("--arm64-ndk", type=Path, help="also build arm64-v8a; checks LOAD alignment (r28+ or r16b)")
    opt = parser.parse_args()
    wire = (ROOT/"java/org/openqpic/localsend/Wire.java").read_text("utf-8")
    methods = {"Java_org_openqpic_localsend_Wire_" + n for n in re.findall(r"native \S+ (\w+)\(", wire)}
    for abi, machine in ABIS.items():
        if abi == "arm64-v8a" and not opt.arm64_ndk:
            continue
        ndk = (opt.arm64_ndk if abi == "arm64-v8a" else opt.ndk).resolve()
        out = ROOT/"build"/abi
        subprocess.run([str(ndk/("ndk-build.cmd" if os.name == "nt" else "ndk-build")),
                        "-j2", "NDK_PROJECT_PATH=.", f"APP_ABI={abi}",
                        f"NDK_OUT={out/'obj'}", f"NDK_LIBS_OUT={out/'stage'}"], cwd=ROOT, check=True)
        elf = out/"stage"/abi/"libqpiclocalsend.so"
        readers = sorted(ndk.glob("toolchains/*/prebuilt/*/bin/*readelf*"))
        tool = next((p for p in readers if "llvm" in p.name), readers[0])
        info = subprocess.check_output([str(tool), "-W", "-h", "-l", "--dyn-syms", str(elf)], text=True)
        exports = set(re.findall(r"GLOBAL\s+DEFAULT\s+\d+\s+(Java_\w+)", info))
        assert exports == methods and re.search(r"Machine:\s+" + machine, info), (abi, exports ^ methods)
        if abi == "arm64-v8a":
            loads = re.findall(r"^\s*LOAD\s+.*\s(0x[0-9a-fA-F]+)\s*$", info, re.M)
            assert loads and all(int(n, 16) >= 16384 for n in loads), loads
        lib = ROOT/"build/libs"/abi/elf.name
        lib.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(elf, lib)
        print(abi, lib.stat().st_size, hashlib.sha256(lib.read_bytes()).hexdigest())


if __name__ == "__main__":
    main()
