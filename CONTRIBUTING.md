# Contributing

Mock Redis uses [git-flow][1] for its branch management. 
We ask that contributors to this project please:

 1. Implement changes in new git branches, following git-flow's model:
 
    -  Changes based off of *develop* will receive the least amount of skepticism.
       
    -  Changes based off of a *release* branches (if one exists) will be considered,
       especially for small bug fixes relevant to the release. We are not likely to 
       accept new features against *release* branches.
       
    -  Changes based off of *master* or a prior release tag will be given the most 
       skepticism. We may accept patches for major bugs against past releases, but
       would prefer to see such changes follow the normal git-flow process.
       
       We will not accept new features based off of *master*.
    
 2. Limit the scope of changes to a single bug fix or feature per branch.
 
 3. Treat documentation and unit tests as an essential part of any change.
 
 4. Update the change log appropriately.

Thank you!

 [1]: https://github.com/nvie/gitflow
