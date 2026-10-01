<!-- knowledge
last_checked: "2026-10-01T11:32:21Z"
-->
# Model Runtime V2

An additive contract package for plugin implementations. Import its public values
from `graphon.model_runtime.v2`. It does not select providers or execute calls.

[Identity records](domain/identity.py) separate `ModelRef` (plugin, provider,
model) from `ContractRef` (contract ID, revision). Contract revisions are opaque
identifiers for effective input/output contracts, not model-weight versions.
Connection selection is separate from model identity.
[Identity checks](../../../../tests/model_runtime/v2/test_identity.py) define
validation, immutability, and exact-string identity semantics.

The public package re-exports the application surface; application declarations
depend on domain records. [Dependency checks](../../../../tests/model_runtime/v2/test_imports.py)
enforce inward imports and standard-library-only dependencies. Importing the
package still executes its existing parent initializer; isolation applies to V2
source dependencies, not the absence of ancestor imports from `sys.modules`.

Existing model consumers require explicit future adapters; introducing these
declarations does not switch any caller to V2.

[DataFormat](domain/formats.py) carries an owned JSON object schema, a tuple of
kind strings, and an optional profile string. Kind and profile values are open
metadata; no shared profile is standardized. [JSON values](domain/json_values.py)
preserve scalar types and require finite numbers, string object keys, and acyclic
lists and objects. The `schema` property returns a defensive copy; callers cannot
change a description by mutating the supplied schema or a returned dictionary.
[Format checks](../../../../tests/model_runtime/v2/test_formats.py) cover these
ownership and JSON boundaries.

The plugin boundary is responsible for interpreting self-contained JSON Schema
Draft 2020-12 with document-local references. Request schemas describe the joint
`{input, parameters}` object; output schemas describe native JSON values. The
declarations check JSON value categories only; schema keywords, validity,
reference policy, and semantic constraints belong to the plugin boundary.
