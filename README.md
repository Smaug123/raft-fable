# Raft

A Raft implementation in F#.
It allows pluggable persistent-store back-ends and communication channels, but out of the box you get an in-memory store and a simulated network.

You can play around with it via a command-line interface (`RaftExplorer`, which you can simply `dotnet run`), or a Fable web UI (`cd RaftFable && npm install && npm start`).

# Development tips

The Node ecosystem being what it is, it is *strongly* recommended that you use the associated Nix flake to give you an environment in which you can run the Fable UI.
Simply `nix develop` (optionally `--profile .profile` if you want to persist the configuration into a [profile](https://nixos.org/manual/nix/stable/package-management/profiles.html) so that it doesn't get garbage-collected).

There are pull request checks on this repo, enforcing [Fantomas](https://github.com/fsprojects/fantomas/)-compliant formatting.
After checking out the repo, you may wish to add a pre-push hook to ensure locally that formatting is complete, rather than having to wait for the CI checks to tell you that you haven't formatted your code.
Consider performing the following command to set this up in the repo:
```bash
git config core.hooksPath hooks/
```
Before your first push (but only once), you will need to install the [.NET local tools](https://docs.microsoft.com/en-us/dotnet/core/tools/local-tools-how-to-use) which form part of the pre-push hook:
```bash
dotnet tool restore
```

