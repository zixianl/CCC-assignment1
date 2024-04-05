# CCC-assignment1

See What Branch You're On: $ git status

Create a New Branch：$ git checkout -b my-branch-name (replacing my-branch-name with whatever name you want)

Switch to a Branch In Your Local Repo：$ git checkout my-branch-name

Pull a Branch: $ git pull origin my-branch-name

Push to a Branch: $ git push -u origin my-branch-name or $ git push -u origin HEAD (If your local branch does not exist on the remote)

$ git push (If your local branch already exists on the remote)

Add files: $ git add .

Delete files: $ git rm filename

Commit files: $ git commit -m "Message that describes what this change does"

Merge a Branch:

First, Must check out the branch that you want to merge another branch into, if not in desired branch: $ git checkout master (Replace master with another branch name as needed)

Now you can merge another branch into the current branch: $ git merge my-branch-name

Delete Branches:

To delete a remote branch, run this command: $ git push origin --delete my-branch-name

To delete a local branch, run either of these commands: $ git branch -d my-branch-name or $ git branch -D my-branch-name

