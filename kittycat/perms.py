import asyncpg
from .kittycat import PartialStaffPosition, StaffPermissions

async def get_user_staff_perms(pool: asyncpg.Pool, user_id: int) -> StaffPermissions:
    user_poses = await pool.fetchrow("SELECT positions, perm_overrides FROM staff_members WHERE user_id = $1", str(user_id))
    position_data = await pool.fetch("SELECT id::text, index, perms FROM staff_positions WHERE id = ANY($1)", user_poses["positions"])

    sp = StaffPermissions(
        perm_overrides=user_poses["perm_overrides"],
        user_positions=[]
    )

    for pos in position_data:
        sp.user_positions.append(
            PartialStaffPosition(
                id=pos["id"],
                index=pos["index"],
                perms=pos["perms"]
            )
        )
    
    return sp