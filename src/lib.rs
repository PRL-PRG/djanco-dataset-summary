use std::path::Path;

use djanco::*;
use djanco::data::*;
use djanco::log::*;
use djanco::csv::*;

use djanco_ext::*;

#[djanco(April, 2020, subsets(Generic))]
pub fn projects(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects().into_csv_in_dir(output, "projects.csv")
}

#[djanco(April, 2020, subsets(Generic))]
pub fn project_commits_all_branches(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    let headers = vec!["project_id",
                       "commit_id", "commit_hash",
                       "author_id", "committer_id",
                       "authored_timestamp", "committed_timestamp",
                       "parent_count", "changed_files_count",
                       "message_length"];

    database.projects()
        .group_by(project::Id)
        .map_into(FromEach(project::Commits,
                           Select!(commit::Id, commit::Hash,
                                   commit::AuthorId, commit::CommitterId,
                                   commit::AuthoredTimestamp, commit::CommittedTimestamp,
                                   Count(commit::Parents), Count(commit::Changes),
                                   commit::MessageLength)))
        .into_csv_with_headers_in_dir(headers, output, "project_commits_all_branches.csv")
}

#[djanco(April, 2020, subsets(Generic))]
pub fn project_users_all_branches(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    let headers = vec!["project_id", "user_id",
                       "authored_commit_count", "committed_commit_count",
                       "author_experience", "committer_experience", "experience"];

    database.projects()
        .group_by(project::Id)
        .map_into(FromEach(project::Users,
                            Select!(user::Id,
                                    Count(user::AuthoredCommits), Count(user::CommittedCommits),
                                    user::AuthorExperience, user::CommitterExperience,
                                    user::Experience)))
        .into_csv_with_headers_in_dir(headers, output, "project_users_all_branches.csv")
}

#[djanco(April, 2020, subsets(Generic))]
pub fn commits(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.commits().into_csv_in_dir(output, "commits.csv")
}

#[djanco(April, 2020, subsets(Generic))]
pub fn users(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.commits().into_csv_in_dir(output, "users.csv")
}