// SPDX-License-Identifier: MIT OR Apache-2.0

use p2panda_auth::Access;
use p2panda_auth::group::GroupMember;
use p2panda_auth::traits::Conditions;

use crate::types::AuthGroupState;
use crate::{ActorId, MemberId};

/// Assign a GroupMember type to passed actor based on looking up if the actor is a group in the
/// auth state.
pub(crate) fn typed_member<C: Conditions>(
    y: &AuthGroupState<C>,
    member: ActorId,
) -> GroupMember<ActorId> {
    if y.members(member).is_empty() {
        GroupMember::Individual(member)
    } else {
        GroupMember::Group(member)
    }
}

/// Assign GroupMember type to every actor based on looking up if the actor is a group in the auth
/// state.
pub(crate) fn typed_members<C: Conditions>(
    y: &AuthGroupState<C>,
    members: Vec<(ActorId, Access<C>)>,
) -> Vec<(GroupMember<ActorId>, Access<C>)> {
    members
        .into_iter()
        .map(|(member, access)| (typed_member(y, member), access))
        .collect()
}

/// The causal cone visible to copies of a group: the group itself plus all
/// its transitive member groups.
///
/// Auth operations may only list dependencies inside their group's visible
/// cone. Copies of a group — one is held by every member of a space built on
/// it — witness exactly the operations of these groups, so a dependency
/// pointing anywhere else could never be resolved there.
pub(crate) fn visible_cone<C: Conditions>(
    y: &AuthGroupState<C>,
    group_id: ActorId,
) -> Vec<ActorId> {
    let mut cone = vec![group_id];
    cone.extend(y.groups(group_id).into_iter().map(|(id, _)| id));
    cone
}

pub(crate) fn sort_members<ID: Ord, C>(members: &mut [(ID, Access<C>)]) {
    members.sort_by(|(actor_a, _), (actor_b, _)| actor_a.cmp(actor_b));
}

pub(crate) fn secret_members<C>(members: Vec<(ActorId, Access<C>)>) -> Vec<ActorId> {
    let mut members: Vec<ActorId> = members
        .into_iter()
        .filter_map(|(id, access)| if access.is_pull() { None } else { Some(id) })
        .collect();
    members.sort();
    members
}

pub(crate) fn added_members(
    current_members: Vec<MemberId>,
    next_members: Vec<MemberId>,
) -> Vec<MemberId> {
    let mut members = next_members
        .iter()
        .cloned()
        .filter(|actor| !current_members.contains(actor))
        .collect::<Vec<_>>();
    members.sort();
    members
}

pub(crate) fn removed_members(
    current_members: Vec<MemberId>,
    next_members: Vec<MemberId>,
) -> Vec<MemberId> {
    let mut members = current_members
        .iter()
        .cloned()
        .filter(|actor| !next_members.contains(actor))
        .collect::<Vec<_>>();
    members.sort();
    members
}
