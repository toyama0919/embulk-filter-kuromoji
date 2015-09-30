# Kuromoji filter plugin for Embulk

Kuromoji filter plugin for Embulk.

see. [Atilika - Applied Search Innovation](http://www.atilika.com/en/products/kuromoji.html)

## Overview

* **Plugin type**: filter

## Configuration

- **key_names**: description (list, required)
- **keep_input**: keep input columns. (bool, default: `true`)
- **ok_parts_of_speech**: ok parts of speech. (list, default: null)
- **dictionary_path**: user dictionary file path. (string, default: null)
- **settings**: description (list, required)
    - **suffix**: output column name suffix. if null overwrite column. (string, default: null)
    - **method**: description (string, required. surface_form or base_form or reading)
    - **delimiter**: delimiter (string, default: ",")

## Example

```yaml
filters:
  - type: kuromoji
    keep_input: false
    ok_parts_of_speech:
      - 名詞
    key_names:
      - catchcopy
    settings:
      - { method: 'reading', delimiter: '' }
      - { suffix: _surface_form_no_delim, method: 'surface_form', delimiter: '' }
      - { suffix: _base_form, method: 'base_form', delimiter: '###' }
      - { suffix: _surface_form, method: 'surface_form', delimiter: '###' }
```

### input

```json
{
    "catchcopy" : "安全・安心を追及した曲面ボディにデザインを一新しました。"
}
```

As below

```json
{
    "catchcopy" : "アンゼン・アンシンヲツイキュウシタキョクメンボディニデザインヲイッシン。",
    "catchcopy_surface_form_no_delim" : "安全・安心を追及した曲面ボディにデザインを一新。",
    "catchcopy_base_form" : "安全###・###安心###を###追及###する###た###曲面###ボディ###に###デザイン###を###一新###。",
    "catchcopy_surface_form" : "安全###・###安心###を###追及###し###た###曲面###ボディ###に###デザイン###を###一新###。"
}
```

## Example2(use user dictionary)

```yaml
  - type: kuromoji
    keep_input: false
    dictionary_path: /tmp/kuromoji.txt
    ok_parts_of_speech:
      - 名詞
    key_names:
      - catchcopy
    settings:
      - { method: 'reading', delimiter: '#' }
      - { suffix: _surface_form_no_delim, method: 'surface_form', delimiter: '' }
      - { suffix: _base_form, method: 'base_form', delimiter: '###' }
      - { suffix: _surface_form, method: 'surface_form', delimiter: '###' }
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
