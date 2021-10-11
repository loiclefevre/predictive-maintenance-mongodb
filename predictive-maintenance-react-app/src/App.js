import './App.css';

function App() {
  // Embedding an APEX application inside a React App
  // Need to configure accordingly the APEX application, look for "Security" in the "Shared Components"
  // On the "Browser Security" tab:
  // - set "Embed in Frames" to "Allow"
  // - set "HTTP Response Headers" to "X-Frame-Options: ALLOW-FROM http://localhost/"
  return (
    <div dangerouslySetInnerHTML={{ __html: "<iframe style='width:100%; height:960px; max-width:100%; margin:auto; display:block;' frameborder='0' src='https://nnrtbqrbdeylh1o-loic.adb-preprod.us-phoenix-1.oraclecloudapps.com/ords/r/predmain/predictive-maintenance-dashboard/login' />"}} />
  );
}

export default App;
