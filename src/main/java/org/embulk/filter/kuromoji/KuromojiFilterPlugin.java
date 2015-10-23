package org.embulk.filter.kuromoji;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.atilika.kuromoji.Token;
import org.atilika.kuromoji.Tokenizer;
import org.atilika.kuromoji.Tokenizer.Builder;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class KuromojiFilterPlugin implements FilterPlugin
{
    public interface PluginTask extends Task
    {
        @Config("key_names")
        public List<String> getKeyNames();

        @Config("dictionary_path")
        @ConfigDefault("null")
        public Optional<String> getDictionaryPath();

        @Config("ok_parts_of_speech")
        @ConfigDefault("null")
        public Optional<List<String>> getOkPartsOfSpeech();
        
        @Config("keep_input")
        @ConfigDefault("true")
        public boolean getKeepInput();

        @Config("settings")
        public List<Map<String, String>> getSettings();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        if (task.getKeepInput()) {
            for (Column inputColumn: inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
                builder.add(outputColumn);
            }
        }

        for (String key: task.getKeyNames()) {
            for (Map<String, String> setting : task.getSettings()) {
                String keyName = key + MoreObjects.firstNonNull(setting.get("suffix"), "");
                if (task.getKeepInput()) {
                    if (setting.get("suffix") != null) {
                        builder.add(new Column(i++, keyName, Types.STRING));
                    }
                } else {
                    builder.add(new Column(i++, keyName, Types.STRING));
                }
            }
        }

        Schema outputSchema = new Schema(builder.build());
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        Builder builder = Tokenizer.builder();
        if (task.getDictionaryPath().isPresent()) {
            try {
                builder.userDictionary(task.getDictionaryPath().get());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        final Tokenizer tokenizer = builder.build();
        final List<Column> keyNameColumns = Lists.newArrayList();

        for (String keyName : task.getKeyNames()) {
            keyNameColumns.add(inputSchema.lookupColumn(keyName));
        }

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    setValue(builder);
                    builder.addRecord();
                }
            }

            /**
             * @param builder
             */
            private void setValue(PageBuilder builder) {
                if (task.getKeepInput()) {
                    for (Column inputColumn: inputSchema.getColumns()) {
                        if (reader.isNull(inputColumn)) {
                            builder.setNull(inputColumn);
                            continue;
                        }
                        if (Types.STRING.equals(inputColumn.getType())) {
                            builder.setString(inputColumn, reader.getString(inputColumn));
                        } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                            builder.setBoolean(inputColumn, reader.getBoolean(inputColumn));
                        } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                            builder.setDouble(inputColumn, reader.getDouble(inputColumn));
                        } else if (Types.LONG.equals(inputColumn.getType())) {
                            builder.setLong(inputColumn, reader.getLong(inputColumn));
                        } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                            builder.setTimestamp(inputColumn, reader.getTimestamp(inputColumn));
                        }
                    }
                }

                for (Column column : keyNameColumns) {
                    List<Token> tokens = tokenizer.tokenize(reader.getString(column));
                    for (Map<String, String> setting: task.getSettings()) {
                        String suffix = setting.get("suffix");
                        String method = setting.get("method");
                        Column outputColumn = outputSchema.lookupColumn(column.getName() + MoreObjects.firstNonNull(suffix, ""));
                        List<String> outputs = Lists.newArrayList();
                        for (Token token : tokens) {
                            if (!isOkPartsOfSpeech(token)) continue;
                            if ("base_form".equals(method)) {
                                outputs.add(MoreObjects.firstNonNull(token.getBaseForm(), token.getSurfaceForm()));
                            } else if ("reading".equals(method)) {
                                outputs.add(MoreObjects.firstNonNull(token.getReading(), token.getSurfaceForm()));
                            } else if ("surface_form".equals(method)) {
                                outputs.add(token.getSurfaceForm());
                            }
                        }
                        Joiner joiner = Joiner.on(MoreObjects.firstNonNull(setting.get("delimiter"), ",")).skipNulls();
                        builder.setString(outputColumn, joiner.join(outputs));
                    }
                }
            }

            private boolean isOkPartsOfSpeech(Token token) {
                if (!task.getOkPartsOfSpeech().isPresent()) return true;
                for (String okPartsOfSpeech : task.getOkPartsOfSpeech().get()) {
                    if (token.getAllFeaturesArray()[0].equals(okPartsOfSpeech)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
