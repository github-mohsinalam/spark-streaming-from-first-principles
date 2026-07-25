# Local Apache Spark Development Environment on Windows

**WSL2 + Ubuntu + JDK 17 + sbt + IntelliJ IDEA (Scala / Spark)**

This guide builds a complete environment for developing and running Apache Spark
applications in Scala (built with `sbt`) on a Windows PC — without installing Spark,
Java, or the IDE natively on Windows. Everything, including IntelliJ IDEA itself, runs
inside WSL2 (a real Linux environment). Keeping the IDE on the same side as the
toolchain avoids an entire class of Windows-to-Linux errors.

> **Assumptions:** Windows 11 with administrator access and an internet connection.
> Spark 4.x is used as the example, which requires JDK 17+ and Scala 2.13.

> **Convention:** Most commands run in the **Ubuntu (WSL) terminal**. A few run in
> **Windows PowerShell** and are marked as such. Replace `<placeholders>` with your
> own values.

## Contents

1. [Install WSL2 and Ubuntu](#1-install-wsl2-and-ubuntu)
2. [Update Ubuntu and install base tools](#2-update-ubuntu-and-install-base-tools)
3. [Install JDK 17 and sbt (via SDKMAN)](#3-install-jdk-17-and-sbt-via-sdkman)
4. [Set up GitHub authentication (SSH)](#4-set-up-github-authentication-ssh)
5. [Clone your project into the WSL filesystem](#5-clone-your-project-into-the-wsl-filesystem)
6. [Install and run IntelliJ IDEA natively in WSL (via WSLg)](#6-install-and-run-intellij-idea-natively-in-wsl-via-wslg)
7. [Configure the project in IntelliJ](#7-configure-the-project-in-intellij)
8. [Verify with a Spark smoke test](#8-verify-with-a-spark-smoke-test)
9. [Appendix A — build.sbt notes for Spark 4 on JDK 17](#appendix-a--buildsbt-notes-for-spark-4-on-jdk-17)

---

## 1. Install WSL2 and Ubuntu

**(PowerShell, as Administrator)**

```powershell
wsl --install
```

Installs the Windows Subsystem for Linux plus the default Ubuntu distribution, and turns
on the required Windows features automatically.

Restart your computer when prompted. After the restart, Ubuntu launches and asks you to
create a Linux username and password — these are separate from your Windows login.

**(PowerShell)**

```powershell
wsl -l -v
```

Lists your distributions and their WSL version. Confirm the distro shows `VERSION 2` —
WSL2 is required (WSL1 is slower and less compatible).

---

## 2. Update Ubuntu and install base tools

```bash
sudo apt update && sudo apt upgrade -y
```

Refreshes the package index and upgrades installed packages to current versions.

```bash
sudo apt install -y zip unzip curl git
```

Installs core tools: `zip`/`unzip` (required by SDKMAN), `curl` (for downloads), and
`git` (source control). **SDKMAN fails to install without `unzip`.**

---

## 3. Install JDK 17 and sbt (via SDKMAN)

SDKMAN manages JVM tools cleanly and keeps everything inside your Linux home directory.

```bash
curl -s "https://get.sdkman.io" | bash
```

Downloads and runs the SDKMAN installer.

```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
```

Loads SDKMAN into the current shell so the `sdk` command works right away. New terminals
load it automatically from `~/.bashrc`.

```bash
sdk list java | grep tem
sdk install java 17.0.19-tem
```

First lists available Eclipse Temurin builds; then installs a Temurin JDK 17 and sets it
as default. Substitute the exact `17.x` identifier the list shows. **Spark 4.x requires
Java 17 or newer.**

```bash
sdk install sbt
```

Installs `sbt`, the Scala build tool.

```bash
java -version
sbt --version
```

Verifies the toolchain: `java` should report `17.x`, and `sbt` should print its launcher
version (the project-specific sbt version is fetched on first build).

---

## 4. Set up GitHub authentication (SSH)

SSH keys avoid repeated passwords and token expiry. (A Personal Access Token over HTTPS
is a fine alternative if your organization blocks SSH.)

```bash
ssh-keygen -t ed25519 -C "<your_email>"
```

Generates a new SSH key pair. Press Enter to accept the default location; a passphrase is
optional but recommended.

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
```

Starts the SSH agent and loads your new private key into it.

```bash
clip.exe < ~/.ssh/id_ed25519.pub
```

Copies the public key to the Windows clipboard. (Or run `cat ~/.ssh/id_ed25519.pub` and
copy it manually.)

In GitHub: **Settings → SSH and GPG keys → New SSH key** → paste → save.

```bash
ssh -T git@github.com
```

Tests the connection. A *"successfully authenticated"* message means it works. GitHub
also says it *does not provide shell access* — that line is expected and correct, **not
an error**.

---

## 5. Clone your project into the WSL filesystem

```bash
mkdir -p ~/projects && cd ~/projects
```

Creates and enters a `projects` folder in your Linux home directory.

```bash
git clone git@github.com:<user>/<repo>.git
```

Clones your repository over SSH.

> **Important:** Keep the project inside your Linux home (`~/...`), **NOT** under
> `/mnt/c/`. Files under `/mnt/c` live on the Windows filesystem, and the cross-boundary
> access makes sbt and Spark extremely slow.

```bash
git config --global core.autocrlf input
```

Keeps Unix line endings (LF) in the Linux repo, avoiding line-ending noise in diffs.

---

## 6. Install and run IntelliJ IDEA natively in WSL (via WSLg)

> **Why inside WSL:** IntelliJ running on Windows cannot cleanly execute a JDK that lives
> in Linux — it tries to launch `java.exe` and fails. IntelliJ **Community Edition** also
> cannot host a Remote-Development backend in WSL. Running IntelliJ itself inside Linux
> removes the boundary entirely. On Windows 11 the Linux-GUI layer (**WSLg**) is already
> built in, so no separate X-server is needed.

**(PowerShell)**

```powershell
wsl --update
```

Updates the WSL kernel and WSLg so Linux GUI apps display correctly.

```bash
sudo apt install -y libxrender1 libxtst6 libxi6 libxext6 fontconfig
```

Installs the graphics and font libraries IntelliJ needs to render its window.

```bash
sudo apt install -y libnss3 libnspr4 libasound2t64
```

Installs the native libraries IntelliJ's bundled Chromium engine (JCEF) depends on.
Without them, JCEF-backed views — most visibly the **Markdown preview** — come up blank.
On Ubuntu 26.04 the ALSA package is `libasound2t64`; on older releases use `libasound2`.
Verify with `ldd ~/idea/jbr/lib/libcef.so | grep "not found"`, which should print nothing.

```bash
cd ~
curl -L -o ideaIC.tar.gz "https://download.jetbrains.com/product?code=IIC&latest&distribution=linux"
```

Downloads the latest IntelliJ IDEA Community Edition for Linux. `code=IIC` selects
Community; `-L` follows the redirect to the actual file.

```bash
mkdir -p ~/idea && tar -xzf ideaIC.tar.gz -C ~/idea --strip-components=1
```

Extracts IntelliJ into `~/idea`.

```bash
echo "fs.inotify.max_user_watches = 524288" | sudo tee -a /etc/sysctl.conf && sudo sysctl -p
```

Raises the limit on watched files so the IDE can monitor a large project without
*"too many open files"* errors.

```bash
~/idea/bin/idea.sh
```

Launches IntelliJ. A native Linux window appears through WSLg. Complete the first-run
wizard.

---

## 7. Configure the project in IntelliJ

1. **Install the Scala plugin.** Settings (`Ctrl+Alt+S`) → Plugins → search "Scala" (by
   JetBrains) → Install → Restart. Adds Scala and sbt language support.
2. **Open the project.** File → Open → `/home/<your-username>/projects/<repo>` — a normal
   Linux path, no `\\wsl...` prefix.
3. **Point it at the WSL JDK.** Project Structure (`Ctrl+Alt+Shift+S`) → SDKs → Add JDK →
   `/home/<your-username>/.sdkman/candidates/java/17.0.19-tem` → set it as Project SDK,
   language level 17. (Find your username with `whoami`.)
4. **Import and run.** Let the sbt import finish, then open the sbt tool window and run
   tasks from its shell (this inherits your build settings — see Appendix A).

> **Tip:** To resize the left Project panel and menus, change the **IDE font** under
> Settings → Appearance & Behavior → Appearance → *Use custom font*. That is separate
> from the **Editor font** (Settings → Editor → Font), which only affects the code area.

---

## 8. Verify with a Spark smoke test

Create `src/main/scala/Main.scala`:

```scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("smoke-test")
      .master("local[*]")
      .getOrCreate()

    println(s">>> Spark version: ${spark.version}")
    spark.stop()
  }
}
```

Then, in the sbt shell:

```bash
run
```

Compiles and runs the app in Spark local mode. Success looks like
`>>> Spark version: 4.1.2` (your version) with no errors — confirming the JDK, sbt, and
Spark are all wired correctly.

> **Why `def main`, not `extends App`:** Scala's `App` trait uses delayed initialization
> that interacts badly with Spark and can hide fields at runtime. A plain `main` method
> avoids the trap.

---

## Appendix A — build.sbt notes for Spark 4 on JDK 17

Two settings are required to run Spark 4.x on Java 17+:

```scala
Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
Compile / run / fork := true
```

The `--add-opens` flags grant Spark access to internal JDK packages that Java 17 locks
down by default; without them you get `java.lang.reflect.InaccessibleObjectException` at
runtime. `fork := true` runs the app in a separate JVM so those options actually take
effect — sbt ignores `javaOptions` for a non-forked run.

> **IntelliJ gotcha:** An IntelliJ "Application" run configuration (the green run arrow)
> does **NOT** read these sbt options. Either run via the sbt shell, or paste the
> `--add-opens` flags into the run configuration's **VM options** field.

### Adding dependencies one at a time

When first setting up, add one library dependency, reload, and run before adding the
next. sbt's resolver reports exactly which artifact fails, so you isolate any problem
immediately instead of debugging the whole set at once.

### Version compatibility (Spark 4.x)

- **Scala 2.13** — Spark 4.x does not support Scala 2.12.
- **JDK 17 or newer.**
- **Delta Lake:** use the Delta release built for your Spark version. From Delta 4.1+,
  Maven artifacts carry a Spark-version suffix (e.g. `delta-spark_4.1_2.13`); if the plain
  coordinate fails to resolve, switch to the suffixed one.

---

*End of guide.*