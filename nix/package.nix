{
  lib,
  stdenvNoCC,
  makeWrapper,
  bash,
  python3,
  nodejs,
  ffmpeg,
  sox,
  pulseaudio,
  gawk,
  coreutils,
  util-linux,
}: let
  python = python3.withPackages (p: [p.pyyaml]);
  runtime = [bash python nodejs ffmpeg sox pulseaudio gawk coreutils util-linux];
  source = lib.cleanSourceWith {
    src = ../.;
    filter = path: type: let
      name = baseNameOf path;
    in
      lib.cleanSourceFilter path type
      && !(builtins.elem name ["result" "__pycache__" "node_modules" ".direnv"]);
  };
in
  stdenvNoCC.mkDerivation {
    pname = "pebble";
    version = "0.1.0";
    src = source;
    nativeBuildInputs = [makeWrapper];
    dontBuild = true;

    installPhase = ''
      runHook preInstall
      mkdir -p $out/bin $out/share/pebble
      cp -r server examples README.md LICENSE $out/share/pebble/
      cp -r docs $out/share/pebble/ 2>/dev/null || true
      rm -rf $out/share/pebble/server/tests
      find $out/share/pebble/server/pi -maxdepth 1 -type f \( -name 'test_*' -o -name '*.test.mjs' \) -delete

      makeWrapper ${python}/bin/python3 $out/bin/pebble-server \
        --add-flags $out/share/pebble/server/receiver.py \
        --prefix PATH : ${lib.makeBinPath runtime}
      makeWrapper ${python}/bin/python3 $out/bin/pebble-actions-check \
        --add-flags $out/share/pebble/server/action_registry.py
      makeWrapper ${python}/bin/python3 $out/bin/pebble-route \
        --add-flags ${../scripts/pebble-route} \
        --prefix PATH : ${lib.makeBinPath [ffmpeg pulseaudio]}
      makeWrapper ${python}/bin/python3 $out/bin/pebble-org-append \
        --add-flags ${../scripts/pebble-org-append}
      install -Dm755 ${../scripts/pebble-chime} $out/bin/pebble-chime
      patchShebangs $out/bin/pebble-chime
      wrapProgram $out/bin/pebble-chime --prefix PATH : ${lib.makeBinPath [sox pulseaudio gawk coreutils]}
      runHook postInstall
    '';

    passthru = {
      inherit python runtime;
    };

    meta = {
      description = "Durable voice and text intake with Jev-powered routing";
      homepage = "https://github.com/harryaskham/pebble";
      license = lib.licenses.mit;
      mainProgram = "pebble-server";
      platforms = lib.platforms.linux;
    };
  }
