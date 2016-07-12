package org.embulk.filter.kuromoji;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseAnalyzer;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.dict.UserDictionary;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.BaseFormAttribute;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.ReadingAttribute;
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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;


public class NeologdPageOutput implements PageOutput
{
    private final KuromojiFilterPlugin.PluginTask task;
    private final List<Column> keyNameColumns;
    private final PageReader reader;
    private final PageBuilder builder;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final JapaneseAnalyzer japaneseAnalyzer;
    private static final Logger logger = Exec.getLogger(KuromojiFilterPlugin.class);

    public NeologdPageOutput(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        this.task = taskSource.loadTask(PluginTask.class);
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.keyNameColumns = Lists.newArrayList();

        for (String keyName : task.getKeyNames()) {
            this.keyNameColumns.add(outputSchema.lookupColumn(keyName));
        }
        this.reader = new PageReader(inputSchema);
        this.builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

        UserDictionary userDict = null;
        try {
            File file = new File(task.getDictionaryPath().get());
            Reader reader = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8);
            userDict = UserDictionary.open(reader);
        } catch (Exception e) {
            logger.error("neologd error", e);
        }

        Mode mode = null;
        if (task.getMode().equals("normal")) {
            mode = JapaneseTokenizer.Mode.NORMAL;
        } else if (task.getMode().equals("search")) {
            mode = JapaneseTokenizer.Mode.SEARCH;
        } else if (task.getMode().equals("extended")) {
            mode = JapaneseTokenizer.Mode.EXTENDED;
        }
        CharArraySet stopSet = JapaneseAnalyzer.getDefaultStopSet();
        Set<String> stopTags = JapaneseAnalyzer.getDefaultStopTags();
        this.japaneseAnalyzer = new JapaneseAnalyzer(userDict, mode, stopSet, stopTags);
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
            List<Token> tokens = tokenize(new StringReader(source));
            for (Map<String, String> setting : task.getSettings()) {
                String suffix = setting.get("suffix");
                String method = setting.get("method");
                Column outputColumn = outputSchema.lookupColumn(column.getName() + MoreObjects.firstNonNull(suffix, ""));
                List<Value> outputs = Lists.newArrayList();
                for (Token token : tokens) {
                    String word = null;
                    if ("base_form".equals(method)) {
                        word = token.getBaseForm();
                    } else if ("reading".equals(method)) {
                        word = token.getReading();
                    } else if ("surface_form".equals(method)) {
                        word = token.getCharTerm();
                    }
                    if (word != null) {
                        outputs.add(ValueFactory.newString(word));
                    }
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
        logger.debug("{} => {}", token.getCharTerm(), token.getPartOfSpeech());
        if (!task.getOkPartsOfSpeech().isPresent()) { return true; };
        for (String okPartsOfSpeech : task.getOkPartsOfSpeech().get()) {
            if (token.getPartOfSpeech().startsWith(okPartsOfSpeech)) {
                return true;
            }
        }
        return false;
    }

    private List<Token> tokenize(Reader reader) {
        List<Token> list = Lists.newArrayList();
        try (TokenStream tokenStream = japaneseAnalyzer.tokenStream("", reader) ) {
            BaseFormAttribute baseAttr = tokenStream.addAttribute(BaseFormAttribute.class);
            CharTermAttribute charAttr = tokenStream.addAttribute(CharTermAttribute.class);
            PartOfSpeechAttribute posAttr = tokenStream.addAttribute(PartOfSpeechAttribute.class);
            ReadingAttribute readAttr = tokenStream.addAttribute(ReadingAttribute.class);

            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                Token token = new Token();
                token.setCharTerm(charAttr.toString());
                token.setBaseForm(baseAttr.getBaseForm());
                token.setReading(readAttr.getReading());
                token.setPartOfSpeech(posAttr.getPartOfSpeech());
                if (!isOkPartsOfSpeech(token)) { continue; }
                list.add(token);
            }
        } catch (Exception e) {
            logger.error("neologd error", e);
        }
        return list;
    }
}
