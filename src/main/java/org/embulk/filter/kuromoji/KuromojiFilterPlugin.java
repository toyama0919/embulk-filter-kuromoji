package org.embulk.filter.kuromoji;

import java.util.List;
import java.util.Map;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

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

        Schema outputSchema = buildOutputSchema(task, inputSchema);

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        return new KuromojiPageOutput(taskSource, inputSchema, outputSchema, output);
    }

    /**
     * @param inputSchema
     * @param task
     * @return
     */
    private Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = buildOutputColumns(task, inputSchema);

        return new Schema(builder.build());
    }

    /**
     * @param task
     * @param inputSchema
     * @return
     */
    private ImmutableList.Builder<Column> buildOutputColumns(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        Map<String, Column> map = Maps.newHashMap();
        int i = 0;
        if (task.getKeepInput()) {
            for (Column inputColumn : inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
                map.put(inputColumn.getName(), outputColumn);
            }
        }

        for (String key : task.getKeyNames()) {
            for (Map<String, String> setting : task.getSettings()) {
                String keyName = key + MoreObjects.firstNonNull(setting.get("suffix"), "");
                Type type = "array".equals(setting.get("type")) ? Types.JSON : Types.STRING;
                map.put(keyName, new Column(i++, keyName, type));
            }
        }

        i = 0;
        for(Map.Entry<String, Column> e : map.entrySet()) {
            final Column column = e.getValue();
            builder.add(new Column(i++, column.getName(), column.getType()));
        }
        return builder;
    }
}
