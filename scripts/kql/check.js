// Parse and analyse the alert and workbook queries against the tables they read.
require("@kusto/language-service-next/bridge.js");
require("@kusto/language-service-next/Kusto.Language.Bridge.js");
const K = Kusto.Language;
const table = new K.Symbols.TableSymbol.$ctor7(
  "ContainerAppConsoleLogs_CL",
  "(TimeGenerated:datetime, Log_s:string, ContainerAppName_s:string, ContainerJobName_s:string, ContainerGroupName_s:string)",
  null);
// The cost and insight tables, from the same file Terraform creates them from.
const custom = require("../../environment/insights/tables.json");
const tables = [table].concat(Object.entries(custom).map(([name, columns]) =>
  new K.Symbols.TableSymbol.$ctor7(
    name, "(" + Object.entries(columns).map(([c, t]) => c + ":" + t).join(", ") + ")", null)));
const db = new K.Symbols.DatabaseSymbol.$ctor3("logs", null, tables, false);
const globals = K.GlobalState.Default.WithDatabase(db);
const queries = require(require("path").resolve(__dirname, process.argv[2]));
let bad = 0;
for (const q of queries) {
  const code = K.KustoCode.ParseAndAnalyze(q, globals);
  const diags = code.GetDiagnostics();
  const errs = [];
  for (let i = 0; i < diags.Count; i++) { const d = diags.getItem(i); if (d.Severity === "Error") errs.push(d.Message + " @" + d.Start); }
  console.log(errs.length ? "ERRORS: " + errs.join(" | ") : "ok", "->", q.split("\n").slice(-1)[0].slice(0, 70));
  bad += errs.length ? 1 : 0;
}
process.exit(bad ? 1 : 0);
