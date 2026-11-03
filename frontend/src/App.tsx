import { BrowserRouter, Route, Routes } from 'react-router-dom'
import AppShell from './components/AppShell'
import RecallPage from './pages/RecallPage'
import TimelinePage from './pages/TimelinePage'
import PatternsPage from './pages/PatternsPage'
import GroupRulesPage from './pages/GroupRulesPage'
import GroupItemsPage from './pages/GroupItemsPage'
import NewGroupPage from './pages/NewGroupPage'
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
          <Route path="/groups/new" element={<NewGroupPage />} />
          <Route path="/groups/:groupId/rules" element={<GroupRulesPage />} />
          <Route path="/groups/:groupId/items" element={<GroupItemsPage />} />
          <Route path="/reflections" element={<ReflectionsPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="*" element={<RecallPage />} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  )
}
