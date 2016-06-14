package org.embulk.filter.kuromoji;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.embulk.config.TaskSource;
import org.embulk.filter.kuromoji.KuromojiFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;

import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.atilika.kuromoji.ipadic.Tokenizer.Builder;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

public class KuromojiPageOutput implements PageOutput
{
    private final PluginTask task;
    private final Tokenizer tokenizer;
    private final List<Column> keyNameColumns;
    private final PageReader reader;
    private final PageBuilder builder;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private static final Logger logger = Exec.getLogger(KuromojiFilterPlugin.class);

    public KuromojiPageOutput(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        this.task = taskSource.loadTask(PluginTask.class);
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;

        Builder builder = new Tokenizer.Builder();
        if (task.getDictionaryPath().isPresent()) {
            try {
                builder.userDictionary(task.getDictionaryPath().get());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.tokenizer = builder.build();
        this.keyNameColumns = Lists.newArrayList();

        for (String keyName : task.getKeyNames()) {
            this.keyNameColumns.add(outputSchema.lookupColumn(keyName));
        }
        this.reader = new PageReader(inputSchema);
        this.builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
    }

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
            for (Column inputColumn : inputSchema.getColumns()) {
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
                } else if (Types.JSON.equals(inputColumn.getType())) {
                    builder.setJson(inputColumn, reader.getJson(inputColumn));
                }
            }
        }

        for (Column column : keyNameColumns) {
            final String source = reader.getString(column);
            List<Token> tokens = tokenizer.tokenize(source);
            logger.debug("{} => {}", source, tokens);
            for (Map<String, String> setting : task.getSettings()) {
                String suffix = setting.get("suffix");
                String method = setting.get("method");
                Column outputColumn = outputSchema.lookupColumn(column.getName() + MoreObjects.firstNonNull(suffix, ""));
                List<Value> outputs = Lists.newArrayList();
                for (Token token : tokens) {
                    logger.debug("token => {}, {}", token, token.getAllFeatures());
                    if (!isOkPartsOfSpeech(token)) { continue; }
                    String word = null;
                    if ("base_form".equals(method)) {
                        word = MoreObjects.firstNonNull(token.getBaseForm(), token.getSurface());
                    } else if ("reading".equals(method)) {
                        word = MoreObjects.firstNonNull(token.getReading(), token.getSurface());
                    } else if ("surface_form".equals(method)) {
                        word = token.getSurface();
                    }
                    outputs.add(ValueFactory.newString(word));
                }
                if (outputColumn.getType().equals(Types.STRING)) {
                    Joiner joiner = Joiner.on(MoreObjects.firstNonNull(setting.get("delimiter"), ",")).skipNulls();
                    builder.setString(outputColumn, joiner.join(outputs));
                } else if (outputColumn.getType().equals(Types.JSON)) {
                    builder.setJson(outputColumn, ValueFactory.newArray(outputs));
                }
            }
        }
    }

    private boolean isOkPartsOfSpeech(Token token) {
        if (!task.getOkPartsOfSpeech().isPresent()) { return true; };
        for (String okPartsOfSpeech : task.getOkPartsOfSpeech().get()) {
            if (token.getAllFeaturesArray()[0].equals(okPartsOfSpeech)) {
                return true;
            }
        }
        return false;
    }
}
