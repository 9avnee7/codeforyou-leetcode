import requests
from concurrent.futures import ThreadPoolExecutor
from warnings import filterwarnings

filterwarnings('ignore')

class LeetcodeScraper:
    def __init__(self):
        self.base_url = 'https://leetcode.com/graphql'

    def scrape_user_profile(self, username):

        output = {}

        def scrape_single_operation(operation):
            json_data = {
                'query': operation_query_dict[operation],
                'variables': {
                    'username': username,
                },
                'operationName': operation,
            }

            try:
                response = requests.post(self.base_url, json=json_data, stream=True, verify=False)
                output[operation] = response.json()['data'] 
            except Exception as e:
                print(f'username: {username}', f'operation: {operation}', f'error: {e}', sep='\n')


        operation_query_dict = {
            'userPublicProfile': '''
                query userPublicProfile($username: String!) {
                  matchedUser(username: $username) {
                    contestBadge {
                      name
                      expired
                      hoverText
                      icon
                    }
                    username
                    githubUrl
                    twitterUrl
                    linkedinUrl
                    profile {
                      ranking
                      userAvatar
                      realName
                      aboutMe
                      school
                      websites
                      countryName
                      company
                      jobTitle
                      skillTags
                      postViewCount
                      postViewCountDiff
                      reputation
                      reputationDiff
                      solutionCount
                      solutionCountDiff
                      categoryDiscussCount
                      categoryDiscussCountDiff
                    }
                  }
                }
            ''',
            'languageStats': '''
                query languageStats($username: String!) {
                  matchedUser(username: $username) {
                    languageProblemCount {
                      languageName
                      problemsSolved
                    }
                  }
                }
            ''',
            'skillStats': '''
                query skillStats($username: String!) {
                  matchedUser(username: $username) {
                    tagProblemCounts {
                      advanced {
                        tagName
                        tagSlug
                        problemsSolved
                      }
                      intermediate {
                        tagName
                        tagSlug
                        problemsSolved
                      }
                      fundamental {
                        tagName
                        tagSlug
                        problemsSolved
                      }
                    }
                  }
                }
            ''',
            'userProblemsSolved': '''
                query userProblemsSolved($username: String!) {
                  allQuestionsCount {
                    difficulty
                    count
                  }
                  matchedUser(username: $username) {
                    problemsSolvedBeatsStats {
                      difficulty
                      percentage
                    }
                    submitStatsGlobal {
                      acSubmissionNum {
                        difficulty
                        count
                      }
                    }
                  }
                }
            ''',
            'userBadges': '''
                query userBadges($username: String!) {
                  matchedUser(username: $username) {
                    badges {
                      id
                      name
                      shortName
                      displayName
                      icon
                      hoverText
                      medal {
                        slug
                        config {
                          iconGif
                          iconGifBackground
                        }
                      }
                      creationDate
                      category
                    }
                    upcomingBadges {
                      name
                      icon
                      progress
                    }
                  }
                }
            ''',
            'userProfileCalendar': '''
                query userProfileCalendar($username: String!, $year: Int) {
                  matchedUser(username: $username) {
                    userCalendar(year: $year) {
                      activeYears
                      streak
                      totalActiveDays
                      dccBadges {
                        timestamp
                        badge {
                          name
                          icon
                        }
                      }
                      submissionCalendar
                    }
                  }
                }
            ''',
        }

        # Remove the operations related to recent submissions and contest details
        operations_to_remove = ['recentAcSubmissions', 'userContestRankingInfo']
        for op in operations_to_remove:
            if op in operation_query_dict:
                del operation_query_dict[op]

        with ThreadPoolExecutor(max_workers=len(operation_query_dict)) as executor:
            executor.map(scrape_single_operation, operation_query_dict)

        return output

    def _scrape_single_global_ranking_page(self, page_num, only_user_details=True):
        query = '''
        {
          globalRanking(page: %d) {
            totalUsers
            userPerPage
            rankingNodes {
              ranking
              currentRating
              currentGlobalRanking
              dataRegion
              user {
                username
                nameColor
                activeBadge {
                  displayName
                  icon
                }
                profile {
                  userAvatar
                  countryCode
                  countryName
                  realName
                }
              }
            }
          }
        }
        ''' % page_num
        
        try:
            response = requests.post(self.base_url, json={'query': query}, stream=True, verify=False)
            data = response.json()['data']['globalRanking']
            if only_user_details:
                return data['rankingNodes']
            else:
                return data
        except Exception as e:
            print(f'Error in page number: {page_num}', f'Error: {e}', sep='\n')

    def scrape_all_global_ranking_users(self):
        first_response = self._scrape_single_global_ranking_page(1, only_user_details=False)
        total_leetcode_global_ranking_users = first_response['totalUsers']
        users_per_page = first_response['userPerPage']
        total_global_ranking_pages = total_leetcode_global_ranking_users // users_per_page
        print(f'Total Leetcode users: {total_leetcode_global_ranking_users}', f'Users per page: {users_per_page}', f'Total pages: {total_global_ranking_pages}', sep='\n')

        final_response = first_response['rankingNodes']

        with ThreadPoolExecutor(max_workers=500) as executor:
            pages = range(2, total_global_ranking_pages + 1)
            results = executor.map(self._scrape_single_global_ranking_page, pages)
            for result in results:
                if result:
                    final_response.extend(result)
        
        return {
            'total_global_ranking_users_present': total_leetcode_global_ranking_users,
            'total_global_ranking_users_scraped': len(final_response),
            'total_global_ranking_pages': total_global_ranking_pages,
            'all_global_ranking_users': final_response
        }
