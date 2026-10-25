// Parse and analyse alert queries against the Container Apps console log table.
require("@kusto/language-service-next/bridge.js");
require("@kusto/language-service-next/Kusto.Language.Bridge.js");
const K = Kusto.Language;
const table = new K.Symbols.TableSymbol.$ctor7(
  "ContainerAppConsoleLogs_CL",
  "(TimeGenerated:datetime, Log_s:string, ContainerAppName_s:string, ContainerJobName_s:string, ContainerGroupName_s:string)",
  null);
const db = new K.Symbols.DatabaseSymbol.$ctor3("logs", null, [table], false);
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
