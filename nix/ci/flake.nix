{
  description = "CI Nix flakes";

  inputs.nixpkgs.url = "nixpkgs/nixos-23.05";

  inputs.rust-overlay.url = "github:oxalica/rust-overlay";

  outputs = { self, nixpkgs, rust-overlay }: {
    fdb-rl-7_1_12 =
      let
        pkgs = import nixpkgs {
          system = "x86_64-linux";
        };

        nix-conf = pkgs.writeTextDir "etc/nix/nix.conf" ''
          sandbox = false
          max-jobs = auto
          cores = 0
          trusted-users = root runner
          experimental-features = nix-command flakes
        '';

        systemd-units = builtins.attrValues (import ./systemd { inherit pkgs; });

        nss-files = import ./nss { inherit pkgs; };

        fdb = import ./fdb-7.1 { inherit pkgs; };

        fdb-files = pkgs.callPackage ./fdb-files-7.1 { version = "7.1.12"; };

        fdb-systemd-units = builtins.attrValues fdb-files.systemd_units;
      in
      with pkgs;
      dockerTools.buildImageWithNixDb {
        name = "fdb-rl-7_1_12";
        tag = "latest";

        contents = [
          (symlinkJoin {
            name = "container-symlinks";
            paths = [
              bashInteractive
              cacert
              coreutils
              curl
              findutils
              git
              glibc.bin
              gnugrep
              gnutar
              gzip
              iproute2
              iputils
              nix-conf
              nixUnstable
              shadow
              systemd
              utillinux
              vim
              which
            ]
            ++ systemd-units
            ++ nss-files
            ++ fdb
            ++ fdb-systemd-units;
          })
        ]
        ++ fdb-files.conf;

        runAsRoot = ''
          mkdir -p -m 1777 /tmp

          mkdir -p /usr/bin
          ln -s ${coreutils}/bin/env /usr/bin/env

          touch /etc/machine-id
          mkdir -p /var
          ln -s /run /var/run

          mkdir -p /home/runner/fdb-rl
          chown -R runner:docker /home/runner

          mkdir -p /opt/fdb/log
          mkdir -p /opt/fdb/data

          chown -R fdb:fdb /opt/fdb/conf
          chmod 644 /opt/fdb/conf/fdb.cluster

          chown fdb:fdb /opt/fdb/data
          chown fdb:fdb /opt/fdb/log

          mkdir -p /nix/var/nix/daemon-socket

          systemctl enable fdbcli.service
          systemctl enable foundationdb.service
          systemctl enable nix-daemon.socket
        '';

        config = {
          Cmd = [ "/lib/systemd/systemd" ];

          Env = [
            "NIX_SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
          ];
        };
      };

    push_rustdoc-7_1_12 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          clang
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          protobuf3_21
          (rust-bin.nightly."2023-10-25".default.override {
            extensions = [
              "llvm-tools-preview"
            ];
          })
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        PROTOC = "${protobuf3_21}/bin/protoc";
        PROTOC_INCLUDE = "${protobuf3_21}/include";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";
      };
  };
}
