{
  lib,
  stdenvNoCC,
  makeWrapper,
  python3,
  ffmpeg,
}:
stdenvNoCC.mkDerivation {
  pname = "pebble-client";
  version = "0.1.0";
  src = ../scripts;
  nativeBuildInputs = [makeWrapper];
  dontBuild = true;
  installPhase = ''
    runHook preInstall
    mkdir -p $out/bin
    makeWrapper ${python3}/bin/python3 $out/bin/pebble-route \
      --add-flags $src/pebble-route \
      --prefix PATH : ${lib.makeBinPath [ffmpeg]}
    makeWrapper ${python3}/bin/python3 $out/bin/pebble-org-append \
      --add-flags $src/pebble-org-append
    runHook postInstall
  '';
  meta = {
    description = "Pebble terminal client and Org append helper";
    homepage = "https://github.com/harryaskham/pebble";
    license = lib.licenses.mit;
    mainProgram = "pebble-route";
    platforms = lib.platforms.unix;
  };
}
