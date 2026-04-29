# Graphon DSL OpenAI Slim Example

This example runs a real DSL import workflow:

`start -> llm -> answer`

It uses:

- `graphon.dsl.loads()` as the import surface
- `dify-plugin-daemon-slim` as the local model runtime bridge
- the Dify OpenAI plugin package
- the `gpt-5.4` model declared in `workflow.yml`
- a plain JSON credentials object passed to `loads()`

## Files

- `workflow.yml`: the DSL document
- `workflow.py`: runnable example script
- `slim`: local `dify-plugin-daemon-slim` binary used by this example
- `cred.json.example`: credential file template
- `cred.json`: local credential file

## Run

```bash
cd examples/graphon_dsl_openai_slim
cp cred.json.example cred.json
```

Fill in `cred.json`. If you use an OpenAI-compatible endpoint, set
`openai_api_base`, then run:

```json
{
  "model_credentials": [
    {
      "vendor": "openai",
      "values": {
        "openai_api_key": "<your-api-key>",
        "openai_api_base": "",
        "openai_organization": "",
        "api_protocol": "responses"
      }
    }
  ]
}
```

```bash
python3 workflow.py
```

You can also pass a custom prompt:

```bash
python3 workflow.py "Reply with only the word Graphon."
```

## Notes

- `workflow.py` always uses the `slim` binary in this directory.
- Slim stores downloaded/extracted plugins under this directory's `.slim/plugins`
  cache.
- `cred.json` is local-only and should not be committed.
- `openai_api_base` is passed to the Dify OpenAI plugin as `openai_api_base`.
  The plugin appends `/v1`, so configure the service root.
- Credentials are not printed.
