const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO Redis",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        "getting-started",
        "configuration",
        "codecs",
        "error-handling",
        "async-redis",
        {
          type: "category",
          label: "Commands",
          items: [
            "commands/strings",
            "commands/keys",
            "commands/hashes",
            "commands/lists",
            "commands/sets",
            "commands/sorted-sets",
            "commands/geo",
            "commands/hyperloglog",
            "commands/streams",
            "commands/scripting",
            "commands/connection"
          ]
        },
        "pub-sub",
        "cluster",
        "testing",
        {
          type: "category",
          label: "Guides",
          items: [
            "tutorial-rate-limiter",
            "how-to-cache-with-ttl",
            "how-to-distributed-lock"
          ]
        }
      ]
    }
  ]
};

module.exports = sidebars;
