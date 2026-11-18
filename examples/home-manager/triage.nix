# Several Jev-powered endpoints, each with its own model and action set.
# No Caco or fleet context is required for custom-action routing.
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
    runtimePackages = [transcribe pkgs.curl];
    transcription = {
      provider = "command";
      model = "whisper-1";
      command = ["pebble-transcribe"];
      output = "json";
      language = "en";
      timeout_secs = 60;
      max_audio_secs = 120;
    };
    providerSecretFiles = {
      OPENAI_API_KEY = "/run/user/1000/secrets/openai-api-key";
      OPENROUTER_API_KEY = "/run/user/1000/secrets/openrouter-api-key";
    };
    providerEnvironment.OPENAI_BASE_URL = "https://api.openai.com/v1";

    commands = {
      enable = true;
      jev = {
        provider = "openrouter";
        model = "openai/gpt-4.1-mini";
      };
      extractor.model = "gpt-4.1-mini";
      actionsFile = "/home/me/.config/pebble/actions.yaml";
      routes = {
        "/pebble/route" = {
          action = "route";
          profile = "default";
        };
        "/pebble/work" = {
          action = "route";
          profile = "work";
        };
        "/pebble/home" = {
          action = "route";
          profile = "home";
        };
      };
      jevProfiles = {
        work = {
          model = "openai/gpt-4.1";
          actions = ["note" "reminder"];
        };
        home = {
          model = "openai/gpt-4.1-mini";
          actions = ["automation" "reminder"];
        };
      };
    };
  };
}
