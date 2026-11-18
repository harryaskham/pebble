{
  description = "Pebble: durable voice and text intake with Jev-powered routing";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    linuxSystems = ["x86_64-linux" "aarch64-linux"];
    systems = linuxSystems ++ ["x86_64-darwin" "aarch64-darwin"];
    forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system (import nixpkgs {inherit system;}));
    forLinuxSystems = f: nixpkgs.lib.genAttrs linuxSystems (system: f system (import nixpkgs {inherit system;}));
  in {
    packages = forAllSystems (_: pkgs: let
      client = pkgs.callPackage ./nix/client.nix {};
    in
      {pebble-client = client;}
      // nixpkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux rec {
        pebble = pkgs.callPackage ./nix/package.nix {};
        default = pebble;
      });

    homeManagerModules = rec {
      pebble = {pkgs, lib, ...}: {
        imports = [./nix/home-manager.nix];
        services.pebble.package = lib.mkDefault self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      };
      default = pebble;
    };

    checks = forLinuxSystems (system: pkgs: let
      python = pkgs.python3.withPackages (p: [p.pyyaml]);
      tested = pkgs.runCommand "pebble-tests" {
        nativeBuildInputs = [python pkgs.nodejs pkgs.ffmpeg pkgs.sox pkgs.pulseaudio pkgs.gawk pkgs.coreutils];
      } ''
        cp -r ${./.} source
        chmod -R u+w source
        cd source/server
        export HOME="$TMPDIR/home" PYTHONDONTWRITEBYTECODE=1
        mkdir -p "$HOME"
        python3 -m unittest discover -s tests -p 'test_*.py'
        node --test pi/test_*.test.mjs
        touch $out
      '';
    in {
      inherit tested;
      package = self.packages.${system}.default;
      client = self.packages.${system}.pebble-client;
    });

    formatter = forAllSystems (_: pkgs: pkgs.alejandra);
  };
}
