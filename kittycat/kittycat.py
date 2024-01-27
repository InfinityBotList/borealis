# Converted using codeconvert.ai
from typing import List
from collections import defaultdict
import os

class PartialStaffPosition:
    """A PartialStaffPosition is a partial representation of a staff position"""
    def __init__(self, id: str, index: int, perms: List[str]):
        self.id = id
        """The id of the position"""

        self.index = index
        """The index of the permission. Lower means higher in the list of hierarchy"""

        self.perms = perms
        """The preset permissions of this position"""

class StaffPermissions:
    """
    A set of permissions for a staff member

    This is a list of permissions that the user has
    """
    def __init__(self, user_positions: List[PartialStaffPosition], perm_overrides: List[str]):
        self.user_positions = user_positions
        self.perm_overrides = perm_overrides

    def resolve(self) -> List[str]:
        applied_perms_val = defaultdict(int)
        user_positions = self.user_positions.copy()
        user_positions.append(PartialStaffPosition("perm_overrides", 0, self.perm_overrides.copy()))
        
        # Sort the positions by index in descending order
        user_positions.sort(key=lambda x: x.index, reverse=True)

        for pos in user_positions:
            for perm in pos.perms:
                if perm.endswith(".@clear"):
                    # Split permission by namespace
                    perm_split = perm.split('.')
                    if len(perm_split) < 2:
                        # Then assume its a global permission on the namespace
                        perm_split = ["global", "@clear"]

                    perm_namespace = perm_split[0]
                    if perm_namespace == "global":
                        # Clear all perms
                        applied_perms_val.clear()
                        continue

                    # Clear all perms with this namespace
                    to_remove = []
                    for key in applied_perms_val:
                        key_split = key.split('.')
                        if len(key_split) < 2:
                            key_split = ["global", "*"]
                        key_namespace = key_split[0]
                        if key_namespace == perm_namespace:
                            to_remove.append(key)
                    
                    # RUST OPT: Remove here to avoid immutable borrow
                    for key in to_remove:
                        del applied_perms_val[key]
                    continue

                if perm.startswith('~'):
                    # Check what gave the permission. We *know* its sorted so we don't need to do anything but remove if it exists
                    if perm[1:] in applied_perms_val:
                        # Remove old permission
                        del applied_perms_val[perm[1:]]
                        # Add the negator
                        applied_perms_val[perm] = pos.index
                    else:
                        if perm in applied_perms_val:
                            # Case 3: The negator is already applied, so we can ignore it
                            continue
                            
                        # Then we can freely add the negator
                        applied_perms_val[perm] = pos.index
                else:
                    # Special case: If a * element exists for a smaller index, then the negator must be ignored. E.g. manager has ~rpc.PremiumAdd but head_manager has no such negator
                    if perm.endswith(".*"):
                        # Remove negators. As the permissions are sorted, we can just check if a negator is in the hashmap
                        perm_split = perm.split('.')
                        perm_namespace = perm_split[0]

                        # If the * element is from a permission of lower index, then we can ignore this negator
                        to_remove = []
                        for key in applied_perms_val:
                            if not key.startswith('~'):
                                continue # This special case only applies to negators

                            key_namespace = key.split('.')[0][1:]

                            # Same namespaces
                            if key_namespace == perm_namespace: 
                                # Then we can ignore this negator
                                to_remove.append(key)

                        # RUST OPT: Remove here to avoid immutable borrow
                        for key in to_remove:
                            del applied_perms_val[key]
                    
                    # If its not a negator, first check if there's a negator
                    if f"~{perm}" in applied_perms_val:
                        # Remove the negator
                        del applied_perms_val[f"~{perm}"]
                        # Add the permission
                        applied_perms_val[perm] = pos.index
                    else:
                        # Case 3: The permission is already applied, so we can ignore it
                        if perm in applied_perms_val:
                            continue
                        
                        # Then we can freely add the permission
                        applied_perms_val[perm] = pos.index

        applied_perms = list(applied_perms_val.keys())

        if os.getenv("DEBUG") == "true":
            print(f"Applied perms: {applied_perms} with hashmap: {applied_perms_val}");

        return applied_perms

def has_perm(perms: List[str], perm: str) -> bool:
    """
    Check if the user has a permission given a set of user permissions and a permission to check for
    
    This assumes a resolved set of permissions
    """
    perm_split = perm.split('.')
    if len(perm_split) < 2:
        # Then assume its a global permission on the namespace
        perm_split = [perm, "*"]
    
    perm_namespace = perm_split[0]
    perm_name = perm_split[1]
    has_perm = None
    has_negator = False
    for user_perm in perms:
        if user_perm == "global.*":
            # Special case
            return True
        
        user_perm_split = user_perm.split('.')
        if len(user_perm_split) < 2:
            # Then assume its a global permission
            user_perm_split = [user_perm, "*"]

        user_perm_namespace = user_perm_split[0]
        user_perm_name = user_perm_split[1]

        if user_perm.startswith('~'):
            # Strip the ~ from namespace to check it
            user_perm_namespace = user_perm_namespace[1:]

        if (user_perm_namespace == perm_namespace or user_perm_namespace == "global") and (user_perm_name == "*" or user_perm_name == perm_name):
            has_perm = user_perm_split
            if user_perm.startswith('~'):
                has_negator = True # While we can optimize here by returning false, we may want to add more negation systems in the future

    return has_perm is not None and not has_negator

def build(namespace: str, perm: str) -> str:
    """Builds a permission string from a namespace and permission"""
    return f"{namespace}.{perm}"

# Checks whether or not a resolved set of permissions allows the addition or removal of a permission to a position
def check_patch_changes(manager_perms: List[str], current_perms: List[str], new_perms: List[str]) -> None:
    """Checks whether or not a resolved set of permissions allows the addition or removal of a permission to a position"""
    
    # Take the symmetric_difference between current_perms and new_perms
    hset_1 = set(current_perms)
    hset_2 = set(new_perms)
    changed = list(hset_2.symmetric_difference(hset_1))
    for perm in changed:
        resolved_perm = perm

        if perm.startswith('~'):
            # Strip the ~ from namespace to check it
            resolved_perm = perm[1:]
        
        if not has_perm(manager_perms, resolved_perm):
            # Check if the user has the permission
            raise Exception(f"You do not have permission to add this permission: {resolved_perm}")
        
        if perm.endswith(".*"):
            perm_split = perm.split('.')
            perm_namespace = perm_split[0] # SAFETY: split is guaranteed to have at least 1 element
            
            # Ensure that new_perms has *at least* negators that manager_perms has within the namespace
            for perms in manager_perms:
                if not perms.startswith('~'):
                    continue # Only check negators
                
                perms_split = perms.split('.')
                perms_namespace = perms_split[0][1:]
                
                if perms_namespace == perm_namespace and perms not in new_perms:
                    raise Exception(f"You do not have permission to add wildcard permission {perm} with negators due to lack of negator {perms}")