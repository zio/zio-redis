const sidebars = {
  sidebar: [
    "index",
    "getting-started",
    {
      type: "category",
      label: "Concepts",
      collapsed: false,
      items: [
        "concepts/codecs",
        "concepts/error-handling",
        "concepts/async-redis",
        "concepts/cluster",
        "concepts/pub-sub"
      ]
    },
    {
      type: "category",
      label: "Reference",
      collapsed: false,
      items: [
        "reference/redis",
        "reference/configuration",
        "reference/embedded-redis",
        {
          type: "category",
          label: "Commands",
          collapsed: false,
          items: [
            "reference/commands/strings",
            "reference/commands/keys",
            "reference/commands/hashes",
            "reference/commands/lists",
            "reference/commands/sets",
            "reference/commands/sorted-sets",
            "reference/commands/geo",
            "reference/commands/hyperloglog",
            "reference/commands/streams",
            "reference/commands/scripting",
            "reference/commands/connection"
          ]
        }
      ]
    },
    {
      type: "category",
      label: "Tutorials",
      collapsed: true,
      items: ["tutorials/rate-limiter", "tutorials/session-store"]
    },
    {
      type: "category",
      label: "How-to Guides",
      collapsed: true,
      items: [
        "how-to/cache-with-ttl",
        "how-to/distributed-lock",
        "how-to/test-with-embedded-redis",
        "how-to/connect-to-a-cluster",
        "how-to/publish-and-subscribe",
        "how-to/pipeline-commands",
        "how-to/connect-with-tls-and-auth"
      ]
    },
    "resources"
  ]
};

module.exports = sidebars;
