//! The Groups screen: consumer groups, their members, and their cursors.

use super::{Column, SELECTED, clamp, column, fitted, selected_index};
use crate::{
    App,
    app::{Pane, Selectable},
    format,
    model::{CursorRow, GroupRow},
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Stylize,
    widgets::{Block, Cell, Table},
};

const CURSOR_COLUMNS: [Column; 5] = [
    column("Partition", 9, true),
    column("Owner", 36, true),
    column("Committed", 11, true),
    column("Next", 11, false),
    column("Lag", 9, true),
];

const GROUP_COLUMNS: [Column; 2] = [column("Group", 5, true), column("Lag", 11, true)];
const MEMBER_COLUMNS: [Column; 3] = [
    column("Member", 6, true),
    column("Client", 36, false),
    column("Owns", 4, true),
];

impl App {
    pub(super) fn draw_groups(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let [panes, detail_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);
        let [list_area, right] =
            Layout::horizontal([Constraint::Length(30), Constraint::Min(0)]).areas(panes);
        let focused = self.focused_style();
        let groups = self.group_rows();
        let count = groups.len();
        let list = groups
            .iter()
            .map(|group| {
                let lag = Cell::from(format::count(group.lag()));
                (group.label.clone(), vec![lag])
            })
            .collect();
        let list = fitted(&GROUP_COLUMNS, list, list_area.width)
            .block(
                Block::bordered()
                    .title(if count == 0 {
                        "Groups (none)"
                    } else {
                        "Groups"
                    })
                    .border_style(focused(Pane::List)),
            )
            .row_highlight_style(SELECTED);
        let selected = groups.get(selected_index(&self.group_table, count));
        let members = selected
            .and_then(|group| group.status)
            .map_or(0, |status| status.members.len());
        let [members_area, cursors_area] =
            Layout::vertical([Constraint::Length(members as u16 + 3), Constraint::Min(0)])
                .areas(right);
        let members_table = members_table(selected, members_area.width);
        let cursor_rows = selected.map_or_else(Vec::new, |group| {
            group
                .cursors
                .iter()
                .map(|row| (row.partition.clone(), cursor_cells(row)))
                .collect()
        });
        let cursor_count = cursor_rows.len();
        let cursors = fitted(&CURSOR_COLUMNS, cursor_rows, cursors_area.width)
            .block(
                Block::bordered()
                    .title("Cursors (record offsets)")
                    .border_style(focused(Pane::Detail)),
            )
            .row_highlight_style(SELECTED);
        let detail = selected.map_or_else(String::new, |group| format!("group {}", group.id));
        self.page = cursors_area.height.saturating_sub(3) as usize;
        clamp(&mut self.group_table, count);
        clamp(&mut self.cursor_table, cursor_count);
        frame.render_stateful_widget(list, list_area, &mut self.group_table);
        self.target_rows(list_area, Selectable::Groups);
        frame.render_widget(members_table, members_area);
        frame.render_stateful_widget(cursors, cursors_area, &mut self.cursor_table);
        self.target_rows(cursors_area, Selectable::Cursors);
        frame.render_widget(detail.dark_gray(), detail_area);
    }
}

/// The members of `group`'s committed plan and how many cursors each owns.
fn members_table(group: Option<&GroupRow<'_>>, width: u16) -> Table<'static> {
    let status = group.and_then(|group| group.status);
    let rows = status
        .iter()
        .flat_map(|status| &status.members)
        .map(|(member, node)| {
            let owns = group.map_or(0, |group| {
                group
                    .cursors
                    .iter()
                    .filter(|row| row.cursor.member == Some(*member))
                    .count()
            });
            let cells = vec![Cell::from(node.to_string()), Cell::from(owns.to_string())];
            (member.to_string(), cells)
        })
        .collect();
    let title = status.map_or_else(
        || "Members".to_owned(),
        |status| format!("Members (plan revision {})", status.revision),
    );
    fitted(&MEMBER_COLUMNS, rows, width).block(Block::bordered().title(title))
}

/// The cells after the partition label.
fn cursor_cells(row: &CursorRow<'_>) -> Vec<Cell<'static>> {
    let owner = row
        .cursor
        .member
        .map_or_else(|| "none".dark_gray(), |member| member.to_string().into());
    let offset = |offset: Option<u64>| offset.map_or_else(|| "-".into(), format::count);
    vec![
        owner.into(),
        Cell::from(offset(row.cursor.offset)),
        Cell::from(format::count(row.next_offset)),
        Cell::from(offset(row.lag())),
    ]
}
