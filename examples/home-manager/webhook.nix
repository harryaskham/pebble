# Audio and text arrive at Pebble. Audio is transcribed by a BYO command, then
# the transcript is sent to a generic webhook.
{inputs, pkgs, ...}: let
  transcribe = pkgs.writeShellApplication {
    name = "pebble-transcribe";
    runtimeInputs = [pkgs.curl];
    text = builtins.readFile ../transcription/openai-compatible.sh;
  };
in {
  imports = [inputs.pebble.homeManagerModules.default];

  services.pebble = {
    enable = true;
    runtimePackages = [transcribe];
    transcription = {
      provider = "command";
      model = "whisper-1";
      command = ["pebble-transcribe"];
      output = "json";
      language = "en";
      timeout_secs = 60;
      max_audio_secs = 120;
    };
    providerSecretFiles.OPENAI_API_KEY = "/run/user/1000/secrets/openai-api-key";
    forward = {
      url = "https://hooks.example.invalid/pebble";
      timeout_secs = 30;
      auth = "bearer-file";
      token_file = "/run/user/1000/secrets/pebble-webhook-token";
      receipt = "http-2xx";
    };
  };
}
