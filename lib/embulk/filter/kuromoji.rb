Embulk::JavaPlugin.register_filter(
  "kuromoji", "org.embulk.filter.kuromoji.KuromojiFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
