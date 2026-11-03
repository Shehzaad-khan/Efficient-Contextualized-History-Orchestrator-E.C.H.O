import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import AppShell from './components/AppShell'
import RecallPage from './pages/RecallPage'
import TimelinePage from './pages/TimelinePage'
import PatternsPage from './pages/PatternsPage'
import GroupsPage from './pages/GroupsPage'
import GroupRulesPage from './pages/GroupRulesPage'
import GroupItemsPage from './pages/GroupItemsPage'
import NewGroupPage from './pages/NewGroupPage'
import SettingsPage from './pages/SettingsPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Reflections became a view of Patterns; the old address keeps working. It redirects outside the
            shell: inside it, the page-change animation never finished after the address changed. */}
        <Route path="/reflections" element={<Navigate to="/patterns?view=reflections" replace />} />
        <Route
          path="*"
          element={
            <AppShell>
              <Routes>
                <Route path="/" element={<RecallPage />} />
                <Route path="/timeline" element={<TimelinePage />} />
                <Route path="/patterns" element={<PatternsPage />} />
                <Route path="/groups" element={<GroupsPage />} />
                <Route path="/groups/new" element={<NewGroupPage />} />
                <Route path="/groups/:groupId/rules" element={<GroupRulesPage />} />
                <Route path="/groups/:groupId/items" element={<GroupItemsPage />} />
                <Route path="/settings" element={<SettingsPage />} />
                <Route path="*" element={<RecallPage />} />
              </Routes>
            </AppShell>
          }
        />
      </Routes>
    </BrowserRouter>
  )
}
