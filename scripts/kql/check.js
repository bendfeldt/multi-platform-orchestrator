// Parse and analyse every KQL this repo ships against the tables it reads:
//   node check.js <queries.json>   alert rules and workbook (extracted from the plan)
// plus fabric/capacity_operations.kql. Exits 1 on any error.
require("@kusto/language-service-next/bridge.js");
require("@kusto/language-service-next/Kusto.Language.Bridge.js");
const fs = require("fs");
const path = require("path");
const K = Kusto.Language;
const repo = path.resolve(__dirname, "../..");

// Columns are built one by one: the schema-string form stops at keyword names like `time`.
function table(name, columns) {
  const symbols = Object.entries(columns).map(([column, type]) => {
    const scalar = K.Symbols.ScalarTypes.GetSymbol(type);
    if (!scalar) throw new Error(`${name}.${column}: unknown type ${type}`);
    return new K.Symbols.ColumnSymbol(column, scalar);
  });
  return new K.Symbols.TableSymbol.$ctor4(name, symbols);
}

function globals(tables) {
  return K.GlobalState.Default.WithDatabase(new K.Symbols.DatabaseSymbol.$ctor3("db", null, tables, false));
}

function errors(text, state) {
  const diagnostics = K.KustoCode.ParseAndAnalyze(text, state).GetDiagnostics();
  const found = [];
  for (let i = 0; i < diagnostics.Count; i++) {
    const d = diagnostics.getItem(i);
    if (d.Severity === "Error") found.push(`${d.Message} @${d.Start}`);
  }
  return found;
}

const logs = globals([
  table("ContainerAppConsoleLogs_CL", {
    TimeGenerated: "datetime", Log_s: "string", ContainerAppName_s: "string",
    ContainerJobName_s: "string", ContainerGroupName_s: "string",
  }),
  // Only the documented columns (container-apps/log-monitoring); jobs: column_ifexists.
  table("ContainerAppSystemLogs_CL", {
    TimeGenerated: "datetime", Log_s: "string", LogLevel_s: "string",
    ContainerAppName_s: "string", RevisionName_s: "string", EnvironmentName_s: "string",
  }),
  // The cost and insight tables, from the same file Terraform creates them from.
  ...Object.entries(require(path.join(repo, "environment/insights/tables.json")))
    .map(([name, columns]) => table(name, columns)),
]);

// fabric/capacity_operations.kql reads the eventstream's table: CloudEvents fields with the
// event's `data` object in one dynamic column (the shape the function documents).
const capacity = globals([
  table("FabricCapacityOperations", {
    source: "string", subject: "string", type: "string", time: "datetime", id: "string",
    specversion: "string", dataschemaversion: "string", data: "dynamic",
  }),
]);

const checks = require(path.resolve(__dirname, process.argv[2])).map((q) => [q, logs]);
const fn = fs.readFileSync(path.join(repo, "fabric/capacity_operations.kql"), "utf8");
checks.push([fn.slice(fn.indexOf("{") + 1, fn.lastIndexOf("}")), capacity]);

let bad = 0;
for (const [query, state] of checks) {
  const found = errors(query, state);
  const last = query.trim().split("\n").slice(-1)[0].slice(0, 70);
  console.log(found.length ? "ERRORS: " + found.join(" | ") : "ok", "->", last);
  bad += found.length ? 1 : 0;
}
process.exit(bad ? 1 : 0);
