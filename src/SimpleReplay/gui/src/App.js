import "@awsui/global-styles/index.css"
import {HomePage} from "./pages/home";
import {AnalysisPage} from "./pages/analysis";
import {BrowserRouter, Routes, Route} from "react-router-dom";

function App() {
  return (
      <BrowserRouter>
          <div className="App">
              <Routes>
                  <Route path="/" element={<HomePage />} />
                  <Route path="/analysis" element={<AnalysisPage />}/>
              </Routes>
            </div>
      </BrowserRouter>
  );
}

export default App;
