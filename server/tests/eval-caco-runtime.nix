pkgs: let
  select = override:
    import ../caco-runtime.nix {
      lib = pkgs.lib;
      inherit override;
    };
in {
  system = select "caco";
  operatorOverride = select "/fixture/override/tool";
  namedOverride = select "custom-caco";
  homeOverride = select "~/bin/custom-caco";
}
