{
  description = "Pebble: durable voice and text intake with Jev-powered routing";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system (import nixpkgs {inherit system;}));
  in {
    packages = forAllSystems (_: pkgs: rec {
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

    checks = forAllSystems (system: pkgs: let
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
    });

    formatter = forAllSystems (_: pkgs: pkgs.alejandra);
  };
}
