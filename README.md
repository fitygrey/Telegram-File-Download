# Telegram File Download

Telegram File Download 是一个本地运行的 Telegram 保存消息媒体下载工具。它通过浏览器里的本地控制台页面管理下载任务，使用 Telegram 二维码登录创建独立会话。

## 功能

- 扫描 Telegram Saved Messages 里的媒体文件
- 按月份查看可下载文件数量
- 批量添加月份到下载队列
- 查看当前下载、待处理列表、跳过项和失败项
- 恢复跳过或失败的下载
- 设置下载目录、文件大小上限和并发数
- 二维码登录和两步验证密码登录流程

## 安装依赖

需要 Python 3。

```bash
python3 -m pip install telethon qrcode
```

## 启动

在项目目录运行：

```bash
cd /path/to/Telegram-File-Download
python3 telegram_dashboard.py
```

启动后控制台会输出访问地址：

```text
Telegram File Download 已启动
登录网址：http://127.0.0.1:8765
关闭这个控制台窗口，或按 Ctrl+C，即可结束程序。
```

用浏览器打开控制台里显示的网址即可。

如果在 macOS 上双击或从没有控制台的环境启动，程序会尝试自动打开 Terminal，并在 Terminal 里输出访问地址。

## 登录

首次打开页面时只会显示登录页。

1. 点击“生成登录二维码”。
2. 用 Telegram 手机 App 扫描二维码并确认登录。
3. 如果账号开启了两步验证，按页面提示输入 Telegram 两步验证密码。
4. 登录成功后会进入主页面。

本项目不从浏览器网络会话、Local Storage 或 IndexedDB 提取 Telegram 登录凭据。请使用项目内二维码登录。

## 下载流程

1. 登录后进入主页面。
2. 在“设置”里确认下载路径、大小上限和并发数。
3. 点击“扫描月份”。
4. 勾选要下载的月份。
5. 点击“添加到下载”。
6. 在“下载”区域查看当前下载和待处理列表。

大小上限为 `0` 表示不限制文件大小。

## 暂停、继续和退出

- “暂停”会暂停当前任务。
- 暂停后按钮会变成“继续”。
- “停止”会停止当前任务。
- 关闭启动程序的控制台窗口，或按 `Ctrl+C`，会结束整个程序。

## 退出登录

右上角点击“退出登录”会清理下载器使用的 Telegram 会话，并返回登录页。退出后需要重新扫码登录。

## 本地数据

以下文件只保存在本机，不会提交到 git：

- Telegram 会话文件：`*.session`
- 下载设置：`dashboard_settings.json`
- 运行日志：`.run/`
- 文档和本地草稿：`docs/`
- 退出登录状态：`revoked_sessions.json`、`session_sign_out.json`

## 运行测试

```bash
python3 -m unittest discover -s tests
```
