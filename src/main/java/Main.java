import server.Server;

private static final String USAGE = """
        Usage: java -jar LaraMQ.jar [--host <address>] [--port <number>]
        
        Defaults:
          --host 127.0.0.1
          --port 3000
        """;

private static boolean hasHelpFlag(String[] args) {
    for (String arg : args) {
        if ("-h".equals(arg) || "--help".equals(arg)) {
            return true;
        }
    }

    return false;
}

void main(String[] args) {
    if (hasHelpFlag(args)) {
        IO.print(USAGE);
        return;
    }

    BrokerConfig config;
    try {
        config = BrokerConfig.parse(args);
    } catch (IllegalArgumentException e) {
        System.err.println(e.getMessage());
        IO.print(USAGE);
        System.exit(1);
        return;
    }

    System.out.printf("Launching LaraMQ broker on %s:%d%n", config.host(), config.port());

    try (Server server = new Server(config.host(), config.port())) {
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "lara-mq-shutdown"));
        server.start();
    } catch (IOException e) {
        System.err.printf("Failed to start broker: %s%n", e.getMessage());
        System.exit(1);
    }
}

private record BrokerConfig(String host, int port) {
    private static BrokerConfig parse(String[] args) {
        String host = Server.DOMAIN;
        int port = Server.PORT;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--host" -> host = requireValue(args, ++i, "--host");
                case "--port" -> port = parsePort(requireValue(args, ++i, "--port"));
                default -> {
                    if (arg.startsWith("--host=")) {
                        host = arg.substring("--host=".length());
                    } else if (arg.startsWith("--port=")) {
                        port = parsePort(arg.substring("--port=".length()));
                    } else if (arg.startsWith("-")) {
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                    } else if (i == 0 && args.length == 1) {
                        port = parsePort(arg);
                    } else {
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                    }
                }
            }
        }

        return new BrokerConfig(host, port);
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + flag);
        }

        return args[index];
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535: " + value);
            }

            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: " + value, e);
        }
    }
}
