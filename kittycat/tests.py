from typing import List
from .kittycat import has_perm, check_patch_changes, StaffPermissions, PartialStaffPosition
import os

def tryer(func):
    try:
        func()
        return True
    except Exception as e:
        print(e)
        return False

def try_check_patch_changes(manager_perms: List[str], current_perms: List[str], new_perms: List[str]) -> None:
    return tryer(lambda: check_patch_changes(manager_perms, current_perms, new_perms))

def test_has_perm():
    assert has_perm(["global.*"], "test") == True
    assert has_perm(["rpc.*"], "global.*") == False
    assert has_perm(["global.test"], "rpc.test") == True
    assert has_perm(["global.test"], "rpc.view_bot_queue") == False
    assert has_perm(["global.*"], "rpc.view_bot_queue") == True
    assert has_perm(["rpc.*"], "rpc.ViewBotQueue") == True
    assert has_perm(["rpc.BotClaim"], "rpc.ViewBotQueue") == False
    assert has_perm(["apps.*"], "rpc.ViewBotQueue") == False
    assert has_perm(["apps.*"], "rpc.*") == False
    assert has_perm(["apps.test"], "rpc.test") == False
    assert has_perm(["apps.*"], "apps.test") == True
    assert has_perm(["~apps.*"], "apps.test") == False
    assert has_perm(["apps.*", "~apps.test"], "apps.test") == False
    assert has_perm(["~apps.test", "apps.*"], "apps.test") == False
    assert has_perm(["apps.test"], "apps.test") == True
    assert has_perm(["apps.test", "apps.*"], "apps.test") == True
    assert has_perm(["~apps.test", "global.*"], "apps.test") == True

    print("has_perm tests passed")

def test_resolve_perms():
    assert StaffPermissions(
        [], 
        ["rpc.test"]
    ).resolve() == ["rpc.test"]
    assert StaffPermissions(
        [
            PartialStaffPosition("test", 1, ["rpc.test"])
        ], 
        []
    ).resolve() == ["rpc.test"]
    assert StaffPermissions(
        [
            PartialStaffPosition("test", 1, ["rpc.test"]), 
            PartialStaffPosition("test2", 2, ["rpc.test2"])
        ],
        []
    ).resolve() == ["rpc.test2", "rpc.test"]
    assert StaffPermissions(
        [
            PartialStaffPosition("test", 1, ["rpc.test", "rpc.test2"]),
            PartialStaffPosition("test2", 2, ["~rpc.test", "~rpc.test3"])
        ], 
        []
    ).resolve() == ["~rpc.test3", "rpc.test", "rpc.test2"]
    assert StaffPermissions(
        [
            PartialStaffPosition("test", 1, ["~rpc.test", "rpc.test2"]),
            PartialStaffPosition("test2", 2, ["~rpc.test3", "rpc.test"])
        ], 
        []
    ).resolve() == ["~rpc.test3", "~rpc.test", "rpc.test2"]
    assert StaffPermissions([PartialStaffPosition("test", 1, ["~rpc.test", "rpc.test2", "rpc.test3"]), PartialStaffPosition("test2", 2, ["~rpc.test3", "~rpc.test2"])], ["rpc.test"]).resolve() == ["rpc.test2", "rpc.test3", "rpc.test"]
    assert StaffPermissions([PartialStaffPosition("test", 1, ["~rpc.test", "rpc.test2"]), PartialStaffPosition("test", 1, ["global.@clear", "~rpc.test", "rpc.test2"]), PartialStaffPosition("test2", 2, ["~rpc.test3", "~rpc.test2"])], ["~rpc.test", "rpc.test2", "rpc.test3"]).resolve() == ["~rpc.test", "rpc.test2", "rpc.test3"]
    assert StaffPermissions([PartialStaffPosition("test", 1, ["rpc.*"])], []).resolve() == ["rpc.*"]
    assert StaffPermissions(
        [
            PartialStaffPosition("test2", 1, ["~rpc.test3", "~rpc.test2"]),
            PartialStaffPosition("test", 2, ["rpc.*"])
        ],
        []
    ).resolve() == ["rpc.*", "~rpc.test3", "~rpc.test2"]
    assert StaffPermissions(
        [
            PartialStaffPosition("reviewer", 1, ["rpc.Claim"])
        ], 
        ["~rpc.Claim"]
    ).resolve() == ["~rpc.Claim"]

    print("resolve_perms tests passed")

def test_check_patch_changes():
    assert try_check_patch_changes(["global.*"], ["rpc.test"], ["rpc.test", "rpc.test2"]) == True
    assert try_check_patch_changes(["rpc.*"], ["global.*"], ["rpc.test", "rpc.test2"]) == False
    assert try_check_patch_changes(["rpc.*"], ["rpc.test"], ["rpc.test", "rpc.test2"]) == True
    assert try_check_patch_changes(["~rpc.test", "rpc.*"], ["rpc.foobar"], ["rpc.*"]) == False
    assert try_check_patch_changes(["~rpc.test", "rpc.*"], ["~rpc.test"], ["rpc.*"]) == False
    assert try_check_patch_changes(["~rpc.test", "rpc.*"], ["~rpc.test"], ["rpc.*", "~rpc.test", "~rpc.test2"]) == True
    assert try_check_patch_changes(["~rpc.test", "rpc.*"], ["~rpc.test"], ["rpc.*", "~rpc.test2", "~rpc.test2"]) == False
    print("check_patch_changes tests passed")

os.environ["DEBUG"] = "true"

for test in [test_has_perm, test_resolve_perms, test_check_patch_changes]:
    test()