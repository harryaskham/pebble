{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.pebble;
  json = pkgs.formats.json {};
  jsonType = json.type;
  stateHome = config.xdg.stateHome;
  configHome = config.xdg.configHome;
  cacheHome = config.xdg.cacheHome;
  source = "${cfg.package}/share/pebble/server";
  python = cfg.package.passthru.python;
  commandEnabled = cfg.commands.enable;
  cacoEnabled = commandEnabled && cfg.commands.caco.enable;
  contextSource = cfg.commands.context.source;
  contextEnabled = commandEnabled && contextSource != "none";
  contextFile = cfg.commands.context.file;
  jevBinary =
    if cfg.commands.jev.package == null
    then "jev"
    else lib.getExe' cfg.commands.jev.package "jev";
  baseSettings = {
    version = 2;
    listen = {
      host = "127.0.0.1";
      port = cfg.port;
      request_timeout_secs = 30;
      max_connections = 4;
    };
    token_file = cfg.tokenFile;
    state_dir = cfg.stateDir;
    max_body_bytes = 8388608;
    max_pending = 128;
    max_archive_bytes = 10737418240;
    min_free_bytes = 1073741824;
    retry_delays_secs = [2 10 30];
    ffmpeg = lib.getExe pkgs.ffmpeg;
    node = lib.getExe pkgs.nodejs;
    transcription = cfg.transcription;
    forward = cfg.forward;
    chime = {
      enabled = cfg.chime.enable;
      argv = [(lib.getExe' cfg.package "pebble-chime")];
      server = cfg.chime.server;
      pitch = 523.25;
      duration = 0.06;
      gain = 0.1;
    };
  };
  commandSettings = {
    enabled = commandEnabled;
    context_file =
      if contextEnabled
      then contextFile
      else "";
    legacy_text_archive = "";
    pebble_project = lib.optionalString cacoEnabled cfg.commands.caco.project;
    pebble_target = lib.optionalString cacoEnabled cfg.commands.caco.target;
    jev = {
      binary = jevBinary;
      inherit (cfg.commands.jev) provider model;
      timeout_secs = cfg.commands.jev.timeoutSecs;
      max_context_age_secs = cfg.commands.jev.maxContextAgeSecs;
    };
    extractor = {
      inherit (cfg.commands.extractor) enabled model;
      base_url = cfg.commands.extractor.baseUrl;
      timeout_secs = cfg.commands.extractor.timeoutSecs;
      max_output_tokens = cfg.commands.extractor.maxOutputTokens;
    };
    caco = {
      enabled = cacoEnabled;
      inherit (cfg.commands.caco) binary;
      daemon_url = cfg.commands.caco.daemonUrl;
      token_file = cfg.commands.caco.tokenFile;
      timeout_secs = cfg.commands.caco.timeoutSecs;
    };
    status.attachment_file = "${stateHome}/pebble/router/pebble-state.json";
    speech = {
      inherit (cfg.commands.speech) enabled provider command voices;
      agent_utils_dir = cfg.commands.speech.agentUtilsDir;
      aztts = cfg.commands.speech.aztts;
      player = cfg.commands.speech.player;
      cache_dir = "${cacheHome}/pebble/speech";
      timeout_secs = cfg.commands.speech.timeoutSecs;
      pulse_server = cfg.commands.speech.pulseServer;
    };
    inherit (cfg.commands) actions routes;
    actions_file = cfg.commands.actionsFile;
    project_aliases = cfg.commands.projectAliases;
    jev_profiles = cfg.commands.jevProfiles;
  };
  settings =
    lib.recursiveUpdate
    (baseSettings // lib.optionalAttrs commandEnabled {commands = commandSettings;})
    cfg.settings;
  configFile = "${configHome}/${cfg.configName}/config.json";
  providerManifest = json.generate "pebble-provider-environment.json" {
    environment = cfg.providerEnvironment;
    secret_files = cfg.providerSecretFiles;
  };
  cacoRuntime = import ../server/caco-runtime.nix {
    inherit lib;
    override = settings.commands.caco.binary or "caco";
  };
  cacoRelay = pkgs.writeShellScriptBin "caco" (builtins.readFile ../server/caco-relay.sh);
  runtimePath = lib.makeBinPath (cfg.package.passthru.runtime ++ [cfg.package] ++ cfg.runtimePackages
    ++ lib.optional (cfg.commands.jev.package != null) cfg.commands.jev.package
    ++ lib.optional (contextEnabled && contextSource == "ctx" && cfg.commands.context.ctx.package != null) cfg.commands.context.ctx.package);
  launcher = pkgs.writeShellScriptBin cfg.commandName ''
    ${lib.optionalString cacoEnabled ''
      if [[ "''${1:-}" == serve ]]; then
        ${cacoRuntime}
      fi
    ''}
    export PATH=${lib.escapeShellArg runtimePath}:"$PATH"
    if [[ "''${1:-}" == serve ]]; then
      ${lib.optionalString cacoEnabled ''export PATH=${cacoRelay}/bin:"$PATH"''}
      exec ${python}/bin/python3 ${source}/provider_env.py --config ${providerManifest} -- \
        ${python}/bin/python3 ${source}/receiver.py --config ${lib.escapeShellArg configFile} "$@"
    fi
    exec ${python}/bin/python3 ${source}/receiver.py --config ${lib.escapeShellArg configFile} "$@"
  '';
  environment =
    lib.mapAttrsToList (name: value: "${name}=${toString value}") cfg.serviceEnvironment
    ++ [
      "CHIME_SOX=${lib.getExe pkgs.sox}"
      "CHIME_PAPLAY=${lib.getExe' pkgs.pulseaudio "paplay"}"
      "CHIME_AWK=${lib.getExe pkgs.gawk}"
    ];
  contextArgs = [
    "--output"
    contextFile
    "--messages"
    (toString cfg.commands.context.messages)
    "--beads"
    (toString cfg.commands.context.beads)
  ];
  contextCommand = lib.escapeShellArgs (
    if contextSource == "ctx"
    then
      [
        "${python}/bin/python3"
        "${source}/context_from_ctx.py"
        "--ctx"
        (lib.getExe' cfg.commands.context.ctx.package "ctx")
        "--ctx-config"
        cfg.commands.context.ctx.configFile
        "--max-age"
        (toString cfg.commands.jev.maxContextAgeSecs)
        "--watch"
      ]
      ++ contextArgs
    else
      [
        "${python}/bin/python3"
        "${source}/context_snapshot.py"
        "--api"
        cfg.commands.caco.daemonUrl
      ]
      ++ contextArgs
      ++ lib.optionals (cfg.commands.caco.tokenFile != "") ["--token-file" cfg.commands.caco.tokenFile]
      ++ lib.optional (!cfg.commands.context.caco.repositoryDescriptions) "--no-repo-descriptions"
  );
  routePaths = builtins.attrNames cfg.commands.routes;
  actionPaths = ["/pebble/action" "/hooks/global/pebble/action"];
  statusPaths = ["/pebble/status" "/pebble/events" "/hooks/global/pebble-status" "/hooks/global/pebble-events" "/route"];
  funnelPaths =
    if cfg.funnel.paths != []
    then cfg.funnel.paths
    else lib.unique ((lib.filter (path: path != "/") routePaths) ++ actionPaths ++ statusPaths);
  commandStopSeconds =
    lib.max
    (4 * settings.commands.speech.timeout_secs + settings.commands.jev.timeout_secs + settings.commands.extractor.timeout_secs + 30)
    (settings.commands.speech.timeout_secs + (lib.max 300 settings.commands.caco.timeout_secs) + 30);
in {
  options.services.pebble = {
    enable = lib.mkEnableOption "Pebble durable audio and text intake";
    package = lib.mkOption {
      type = lib.types.package;
      description = "Pebble package.";
    };
    commandName = lib.mkOption {
      type = lib.types.str;
      default = "pebble";
      description = "Name of the configured service launcher installed in home.packages.";
    };
    configName = lib.mkOption {
      type = lib.types.str;
      default = "pebble";
      description = "XDG config directory for generated config.json and providers.json.";
    };
    units = {
      service = lib.mkOption {
        type = lib.types.str;
        default = "pebble";
      };
      context = lib.mkOption {
        type = lib.types.str;
        default = "pebble-context";
      };
      funnel = lib.mkOption {
        type = lib.types.str;
        default = "pebble-funnel";
      };
    };
    port = lib.mkOption {
      type = lib.types.port;
      default = 11301;
      description = "Loopback HTTP port.";
    };
    stateDir = lib.mkOption {
      type = lib.types.str;
      default = "${stateHome}/pebble/audio";
      description = "Private durable archive directory.";
    };
    tokenFile = lib.mkOption {
      type = lib.types.str;
      default = "${configHome}/pebble/ingress.token";
      description = "Owner-private bearer token accepted by Pebble's HTTP endpoints.";
    };
    transcription = lib.mkOption {
      type = lib.types.nullOr jsonType;
      default = null;
      example = lib.literalExpression ''
        {
          provider = "command";
          model = "local-whisper";
          command = ["/path/to/transcribe" "--json"];
          output = "json";
          language = "en";
          timeout_secs = 60;
          max_audio_secs = 120;
        }
      '';
      description = "Transcription provider settings. `command` receives 24 kHz mono WAV on stdin and prints text or JSON with a `text` field.";
    };
    forward = lib.mkOption {
      type = jsonType;
      default = {
        url = "";
        timeout_secs = 60;
        auth = "none";
        token_file = "";
        receipt = "http-2xx";
      };
      description = "Transcript webhook used when command routing is disabled.";
    };
    chime = {
      enable = lib.mkEnableOption "an immediate upload chime";
      server = lib.mkOption {
        type = lib.types.str;
        default = "";
        description = "Optional PulseAudio server for the chime.";
      };
    };
    providerEnvironment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {};
      description = "Non-secret provider environment. Secret-looking names are rejected; use providerSecretFiles.";
    };
    providerSecretFiles = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {};
      example = {OPENAI_API_KEY = "/run/user/1000/secrets/openai-api-key";};
      description = "Environment variables read from owner-private files when the service starts.";
    };
    serviceEnvironment = lib.mkOption {
      type = lib.types.attrsOf (lib.types.oneOf [lib.types.str lib.types.int lib.types.bool]);
      default = {};
      description = "Additional non-secret service environment, for example adapter configuration.";
    };
    runtimePackages = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [];
      description = "Packages added to the service PATH, such as transcription or action commands.";
    };
    serviceAfter = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      description = "Additional user units ordered before Pebble.";
    };
    serviceConflicts = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      description = "User units that must not run with Pebble, such as preview receivers on the same port.";
    };
    serviceWants = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      description = "Additional user units wanted by Pebble.";
    };
    settings = lib.mkOption {
      type = jsonType;
      default = {};
      description = "Raw receiver settings recursively merged last. Keep secret values out of this option.";
    };
    commands = {
      enable = lib.mkEnableOption "Jev triage endpoints and durable action dispatch";
      routes = lib.mkOption {
        type = lib.types.attrsOf jsonType;
        default = {
          "/" = {
            action = "route";
            profile = "default";
          };
          "/pebble/route" = {
            action = "route";
            profile = "default";
          };
        };
        description = "HTTP path to `{ action, profile }`. Use `route` for Jev triage or `custom:NAME` for a fixed action.";
      };
      jevProfiles = lib.mkOption {
        type = lib.types.attrsOf jsonType;
        default = {};
        description = "Named Jev profiles. Each may override provider/model/timeouts and restrict `actions`.";
      };
      actions = lib.mkOption {
        type = lib.types.attrsOf jsonType;
        default = {};
        description = "Trusted custom actions. Commands receive the extracted payload on stdin.";
      };
      actionsFile = lib.mkOption {
        type = lib.types.str;
        default = "";
        description = "Optional runtime YAML action manifest re-read for each new request.";
      };
      projectAliases = lib.mkOption {
        type = lib.types.attrsOf lib.types.str;
        default = {};
        description = "Explicit spoken project aliases for adapter-backed project routing.";
      };
      jev = {
        package = lib.mkOption {
          type = lib.types.nullOr lib.types.package;
          default = pkgs.jev-cli or null;
          defaultText = lib.literalExpression "pkgs.jev-cli or null";
          description = "Package providing `jev`; null uses `jev` from PATH.";
        };
        provider = lib.mkOption {
          type = lib.types.enum ["official" "openrouter" "vercel" "custom"];
          default = "openrouter";
          description = "Default Jev provider.";
        };
        model = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Default Jev model; null uses Jev's default.";
        };
        timeoutSecs = lib.mkOption {
          type = lib.types.ints.between 1 120;
          default = 20;
        };
        maxContextAgeSecs = lib.mkOption {
          type = lib.types.ints.between 1 3600;
          default = 900;
        };
      };
      extractor = {
        enabled = lib.mkOption {
          type = lib.types.bool;
          default = true;
          description = "Extract payloads before custom actions run.";
        };
        model = lib.mkOption {
          type = lib.types.str;
          default = "gpt-4.1-mini";
        };
        baseUrl = lib.mkOption {
          type = lib.types.str;
          default = "";
          description = "OpenAI-compatible base URL; empty uses OPENAI_BASE_URL.";
        };
        timeoutSecs = lib.mkOption {
          type = lib.types.ints.between 1 120;
          default = 20;
        };
        maxOutputTokens = lib.mkOption {
          type = lib.types.ints.between 128 8192;
          default = 8192;
        };
      };
      speech = {
        enabled = lib.mkOption {
          type = lib.types.bool;
          default = false;
        };
        provider = lib.mkOption {
          type = lib.types.enum ["auto" "agent-utils-azure" "aztts" "command"];
          default = "command";
        };
        command = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [];
          description = "Command speech provider. Text is stdin; PEBBLE_SPEECH_VOICE/SPEED/EMBEDDING are set.";
        };
        agentUtilsDir = lib.mkOption {
          type = lib.types.str;
          default = "";
        };
        aztts = lib.mkOption {
          type = lib.types.str;
          default = "aztts";
        };
        player = lib.mkOption {
          type = lib.types.str;
          default = lib.getExe' pkgs.pulseaudio "paplay";
        };
        timeoutSecs = lib.mkOption {
          type = lib.types.ints.between 1 180;
          default = 120;
        };
        pulseServer = lib.mkOption {
          type = lib.types.str;
          default = "";
        };
        voices = lib.mkOption {
          type = lib.types.attrsOf lib.types.str;
          default = {
            PEBBLE_UI_VOICE = "en-US-Ava";
            PEBBLE_UI_SPEED = "1.0";
            PEBBLE_UI_EMBEDDING = "";
            PEBBLE_SELF_VOICE = "en-US-Andrew";
            PEBBLE_SELF_SPEED = "1.0";
            PEBBLE_SELF_EMBEDDING = "";
          };
        };
      };
      caco = {
        enable = lib.mkEnableOption "the optional Cacophony action adapter";
        binary = lib.mkOption {
          type = lib.types.str;
          default = "caco";
        };
        daemonUrl = lib.mkOption {
          type = lib.types.str;
          default = "http://127.0.0.1:11100";
        };
        tokenFile = lib.mkOption {
          type = lib.types.str;
          default = "";
        };
        timeoutSecs = lib.mkOption {
          type = lib.types.ints.between 1 600;
          default = 130;
        };
        project = lib.mkOption {
          type = lib.types.str;
          default = "";
        };
        target = lib.mkOption {
          type = lib.types.str;
          default = "";
          description = "Assistant target as node:project:agent.";
        };
      };
      context = {
        source = lib.mkOption {
          type = lib.types.enum ["none" "ctx" "caco"];
          default = "none";
          description = "Routing context source. `none` is valid for custom-action-only Jev routes.";
        };
        file = lib.mkOption {
          type = lib.types.str;
          default = "${stateHome}/pebble/context.json";
        };
        messages = lib.mkOption {
          type = lib.types.ints.between 1 100;
          default = 40;
        };
        beads = lib.mkOption {
          type = lib.types.ints.between 1 10;
          default = 3;
        };
        refreshInterval = lib.mkOption {
          type = lib.types.ints.between 30 3600;
          default = 300;
        };
        after = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [];
        };
        ctx = {
          package = lib.mkOption {
            type = lib.types.nullOr lib.types.package;
            default = null;
          };
          configFile = lib.mkOption {
            type = lib.types.str;
            default = "${configHome}/ctx/config.yaml";
          };
        };
        caco.repositoryDescriptions = lib.mkOption {
          type = lib.types.bool;
          default = false;
          description = "Ask gh for repository descriptions when Caco context lacks them.";
        };
      };
    };
    funnel = {
      enable = lib.mkEnableOption "Tailscale Funnel paths for Pebble";
      hostname = lib.mkOption {
        type = lib.types.str;
        default = "";
      };
      package = lib.mkOption {
        type = lib.types.package;
        default = pkgs.tailscale;
      };
      paths = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [];
        description = "Command-mode paths. Empty derives them from routes, actions and status endpoints.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = pkgs.stdenv.hostPlatform.isLinux;
        message = "Pebble's Home Manager service currently requires Linux/systemd.";
      }
      {
        assertion = cfg.transcription != null;
        message = "services.pebble.transcription must select a transcription provider.";
      }
      {
        assertion = commandEnabled || (cfg.forward.url or "") != "";
        message = "services.pebble.forward.url is required unless command routing is enabled.";
      }
      {
        assertion = !cacoEnabled || (cfg.commands.caco.project != "" && cfg.commands.caco.target != "");
        message = "services.pebble.commands.caco.project and target are required when the Caco adapter is enabled.";
      }
      {
        assertion = !(contextEnabled && contextSource == "ctx") || cfg.commands.context.ctx.package != null;
        message = "services.pebble.commands.context.ctx.package is required for ctx context.";
      }
      {
        assertion = !cfg.funnel.enable || cfg.funnel.hostname != "";
        message = "services.pebble.funnel.hostname is required when Funnel is enabled.";
      }
    ];
    home.packages = [cfg.package launcher];
    home.activation.pebbleActionsFile = lib.mkIf (commandEnabled && cfg.commands.actionsFile != "") (lib.hm.dag.entryAfter ["writeBoundary"] ''
      f=${lib.escapeShellArg cfg.commands.actionsFile}
      f="''${f/#\~/$HOME}"
      if [ ! -e "$f" ] && [ -z "''${DRY_RUN_CMD:-}" ]; then
        mkdir -p "$(dirname "$f")"
        (umask 077 && printf '%s\n' '# Pebble runtime actions; see docs/actions.md' 'actions: {}' > "$f")
      fi
    '');
    xdg.configFile."${cfg.configName}/config.json".source = json.generate "pebble-config.json" settings;
    xdg.configFile."${cfg.configName}/providers.json".source = providerManifest;
    systemd.user.services.${cfg.units.service} = {
      Unit = {
        Description =
          if commandEnabled
          then "Pebble audio/text intake and Jev triage"
          else "Pebble audio archive, transcription and forwarding";
        StartLimitIntervalSec = 120;
        StartLimitBurst = 6;
        After = ["network-online.target"] ++ cfg.serviceAfter;
        Wants = cfg.serviceWants;
        Conflicts = cfg.serviceConflicts;
      };
      Service = {
        Type = "simple";
        ExecStart = "${launcher}/bin/${cfg.commandName} serve";
        ExecStartPost = "${launcher}/bin/${cfg.commandName} health --wait 30";
        Environment = environment;
        UMask = "0077";
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectSystem = "full";
        MemoryMax = "768M";
        TasksMax = 64;
        CPUQuota = "100%";
        Restart = "on-failure";
        RestartPreventExitStatus = [78];
        RestartSec = 5;
        TimeoutStartSec = 45;
        TimeoutStopSec =
          if commandEnabled
          then lib.max 210 commandStopSeconds
          else 210;
      };
      Install.WantedBy = ["default.target"];
    };
    systemd.user.services.${cfg.units.context} = lib.mkIf contextEnabled {
      Unit = {
        Description =
          if contextSource == "ctx"
          then "Project ctx inventory into Pebble routing context"
          else "Refresh Pebble routing context from Caco";
        After = ["network-online.target"] ++ cfg.commands.context.after;
      };
      Service =
        {
          Type =
            if contextSource == "ctx"
            then "simple"
            else "oneshot";
          ExecStart = contextCommand;
          Environment =
            if contextSource == "ctx"
            then ["PATH=${runtimePath}"]
            else environment;
          UMask = "0077";
          NoNewPrivileges = true;
          PrivateTmp = true;
          ProtectSystem = "full";
          MemoryMax = "512M";
          TasksMax = 32;
          CPUQuota = "50%";
          TimeoutStartSec =
            if contextSource == "ctx"
            then 45
            else 240;
        }
        // lib.optionalAttrs (contextSource == "ctx") {
          Restart = "on-failure";
          RestartSec = 5;
          TimeoutStopSec = 10;
        };
      Install = lib.mkIf (contextSource == "ctx") {WantedBy = ["default.target"];};
    };
    systemd.user.timers.${cfg.units.context} = lib.mkIf (contextEnabled && contextSource == "caco") {
      Unit.Description = "Refresh Pebble routing context";
      Timer = {
        OnBootSec = "15s";
        OnUnitInactiveSec = "${toString cfg.commands.context.refreshInterval}s";
        Unit = "${cfg.units.context}.service";
      };
      Install.WantedBy = ["timers.target"];
    };
    systemd.user.services.${cfg.units.funnel} = lib.mkIf cfg.funnel.enable {
      Unit = {
        Description = "Reconcile Pebble paths on an existing Tailscale Funnel";
        After = ["${cfg.units.service}.service"];
        Wants = ["${cfg.units.service}.service"];
        StartLimitIntervalSec = 120;
        StartLimitBurst = 3;
      };
      Service = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = lib.escapeShellArgs ([
            "${python}/bin/python3"
            "${source}/funnel.py"
            "--tailscale"
            (lib.getExe cfg.funnel.package)
            "--hostname"
            cfg.funnel.hostname
            "--backend"
            "http://127.0.0.1:${toString cfg.port}"
            "--apply"
            (
              if commandEnabled
              then "--commands"
              else "--retire-commands"
            )
          ]
          ++ lib.concatMap (path: ["--path" path]) (lib.optionals commandEnabled funnelPaths));
        Restart = "on-failure";
        RestartSec = 10;
        TimeoutStartSec = 60;
      };
      Install.WantedBy = ["default.target"];
    };
  };
}
