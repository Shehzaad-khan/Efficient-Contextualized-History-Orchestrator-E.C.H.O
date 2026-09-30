import { BrowserRouter, Route, Routes } from 'react-router-dom'
import AppShell from './components/AppShell'
import RecallPage from './pages/RecallPage'
import TimelinePage from './pages/TimelinePage'
import PatternsPage from './pages/PatternsPage'
import ReflectionsPage from './pages/ReflectionsPage'
import SettingsPage from './pages/SettingsPage'

export default function App() {
  return (
    <BrowserRouter>
      <AppShell>
        <Routes>
          <Route path="/" element={<RecallPage />} />
          <Route path="/timeline" element={<TimelinePage />} />
          <Route path="/patterns" element={<PatternsPage />} />
          <Route path="/reflections" element={<ReflectionsPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="*" element={<RecallPage />} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  )
}
