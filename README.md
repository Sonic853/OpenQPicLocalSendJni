# OpenQPicLocalSend

OpenQPic 的 LocalSend v2.2 明文 HTTP 传输组件。Android 独立库名为 **`libqpiclocalsend.so`**，不含 Flutter、Rust、TLS 或 C++ 运行时。图像处理仍由另一仓库 **OpenQPicJni** 提供。

- `src/transport.c`：IPv4 单接口 UDP 组播、非阻塞监听、TCP 客户端、HTTP 头与定长/chunked 解码、64 位 FD 流和节流进度回调。`third_party/picohttpparser` 固定上游提交，许可见 `NOTICE`。
- `java/org/openqpic/localsend/`：JNI 声明、设备公告和 HTTP 回注册、`info/register/prepare-upload/upload/cancel`、逐文件 token、用户确认、PIN、并发接收与校验重试。所有线上文本使用标准 UTF-8。`/v1/info` 仅用于旧客户端探测。
- 应用负责用户界面、选择网卡、`MulticastLock`、URI 授权、持有 FD、校验本地文件、完成后向 MediaStore/下载目录发布。文件正文不经过 JNI 字节数组。

一个前台界面创建一个 `LocalSend`。`close()` 同步关闭两个监听入口，唤醒待确认请求并中断活跃连接。只在已有会话需要时通知对端取消。没有进程级守护服务、开机广播或后台扫描。

## 构建

```sh
python build_android.py --ndk /path/to/android-ndk-r16b --arm64-ndk /path/to/android-ndk-r28c
```

r16b 构建 `armeabi` 和 `x86`；指定 r28c 时另产出 16 KiB 页对齐的 `arm64-v8a`。产物在 `build/libs/<abi>/libqpiclocalsend.so`。脚本核对 ELF 机器、JNI 导出和 arm64 的 LOAD 对齐。也可从 Android NDK CMake 工程 `add_subdirectory` 本仓库并构建 `qpiclocalsend` 目标。

OpenQPic 应用运行 `python tools/sync_localsend.py` 同步库和匹配的 Java 桥；修改此处的桥后需一同同步。应用常规离线构建只依赖它自己的已固定输入。

## 官方协议互通测试

```sh
python tests/official_peer.py /path/to/localsend
```

已验证的上游提交为 `d8702511dfb074d795452a9204ba3c9473f4f01f`。测试工具在 `build/official` 内用上游 HTTP 客户端和服务端编译测试对端，不改动上游仓库。配合 OpenQPic 的 `tools/localsend_smoke.py` 在独立 API 23 ARM 模拟器上运行。测试中的 Rust 依赖不进入 Android 包。
