package org.embulk.filter.kuromoji;

public class Token
{
    private String charTerm;
    private String baseForm;
    private String partOfSpeech;
    private String reading;
    private String inflection;

    public String getCharTerm()
    {
        return charTerm;
    }
    public String getBaseForm()
    {
        return baseForm;
    }
    public String getPartOfSpeech()
    {
        return partOfSpeech;
    }
    public void setCharTerm(String charTerm)
    {
        this.charTerm = charTerm;
    }
    public void setBaseForm(String baseForm)
    {
        this.baseForm = baseForm;
    }
    public void setPartOfSpeech(String partOfSpeech)
    {
        this.partOfSpeech = partOfSpeech;
    }
    public void setReading(String reading)
    {
        this.reading = reading;
    }
    public String getReading()
    {
        return reading;
    }
    public String getInflection()
    {
        return inflection;
    }
    public void setInflection(String inflection)
    {
        this.inflection = inflection;
    }
}
