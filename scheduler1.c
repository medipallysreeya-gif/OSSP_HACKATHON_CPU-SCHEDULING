
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>

#define MAX_PROCESSES 50
#define MAX_INPUT_TIME 10000
#define AGING_INTERVAL 3

typedef struct {
    int pid;
    int arrival;
    int burst;
    int remaining;
    int priority;
    int completion;
    int first_start;
    int finished;
} Process;

typedef struct {
    int *timeline;
    int length;
    int capacity;
} Schedule;

typedef struct {
    double avg_waiting;
    double avg_turnaround;
    double avg_response;
} Statistics;

typedef struct {
    Process *processes;
    int n;
    Statistics *result;
} ThreadArgs;

pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;



int effective_priority(Process *p, int time, int use_aging) {
    int executed = p->burst - p->remaining;
    int waiting = time - p->arrival - executed;

    if (use_aging)
        return p->priority + waiting / AGING_INTERVAL;

    return p->priority;
}

int select_process(Process p[], int n, int time, int use_aging) {
    int selected = -1;
    int best_priority = -1;

    for (int i = 0; i < n; i++) {
        if (p[i].finished || p[i].arrival > time)
            continue;

        int priority = effective_priority(&p[i], time, use_aging);

        if (selected == -1 ||
            priority > best_priority ||
            (priority == best_priority &&
             p[i].arrival < p[selected].arrival) ||
            (priority == best_priority &&
             p[i].arrival == p[selected].arrival &&
             p[i].pid < p[selected].pid)) {

            selected = i;
            best_priority = priority;
        }
    }

    return selected;
}

int run_scheduler(Process p[], int n, Schedule *schedule,
                  int use_aging) {
    int time = 0;
    int completed = 0;

    while (completed < n) {
        int selected = select_process(p, n, time, use_aging);

        if (selected == -1) {
            schedule->timeline[time++] = 0;
            continue;
        }

        Process *current = &p[selected];

        if (current->first_start == -1)
            current->first_start = time;

        schedule->timeline[time] = current->pid;
        current->remaining--;
        time++;

        if (current->remaining == 0) {
            current->completion = time;
            current->finished = 1;
            completed++;
        }

        if (time >= schedule->capacity) {
            fprintf(stderr, "Schedule capacity exceeded.\n");
            return -1;
        }
    }

    schedule->length = time;
    return 0;
}



Process *copy_processes(Process original[], int n) {
    Process *copy = malloc(n * sizeof(Process));

    if (copy == NULL) {
        perror("malloc");
        return NULL;
    }

    for (int i = 0; i < n; i++) {
        copy[i] = original[i];
        copy[i].remaining = copy[i].burst;
        copy[i].completion = 0;
        copy[i].first_start = -1;
        copy[i].finished = 0;
    }

    return copy;
}



void display_gantt(Schedule *s) {
    printf("\nCompressed Gantt Chart:\n");

    int start = 0;

    while (start < s->length) {
        int end = start + 1;

        while (end < s->length &&
               s->timeline[end] == s->timeline[start]) {
            end++;
        }

        if (s->timeline[start] == 0)
            printf("| IDLE (%d-%d) ", start, end);
        else
            printf("| P%d (%d-%d) ",
                   s->timeline[start], start, end);

        start = end;
    }

    printf("|\n");
}



void *calculate_statistics(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;

    double waiting = 0;
    double turnaround = 0;
    double response = 0;

    for (int i = 0; i < args->n; i++) {
        Process *p = &args->processes[i];

        int tat = p->completion - p->arrival;
        int wt = tat - p->burst;
        int rt = p->first_start - p->arrival;

        waiting += wt;
        turnaround += tat;
        response += rt;
    }

    pthread_mutex_lock(&stats_mutex);

    args->result->avg_waiting = waiting / args->n;
    args->result->avg_turnaround = turnaround / args->n;
    args->result->avg_response = response / args->n;

    pthread_mutex_unlock(&stats_mutex);

    return NULL;
}

int get_statistics(Process p[], int n, Statistics *result) {
    pthread_t thread;
    ThreadArgs args = {p, n, result};

    if (pthread_create(&thread, NULL,
                       calculate_statistics, &args) != 0) {
        fprintf(stderr, "Unable to create statistics thread.\n");
        return -1;
    }

    if (pthread_join(thread, NULL) != 0) {
        fprintf(stderr, "Unable to join statistics thread.\n");
        return -1;
    }

    return 0;
}

/* ---------- Display and report ---------- */

void display_results(FILE *out, const char *title,
                     Process p[], int n, Schedule *s,
                     Statistics *stats) {
    fprintf(out, "\n===== %s =====\n", title);

    if (out == stdout)
        display_gantt(s);
    else {
        fprintf(out, "Compressed Gantt Chart:\n");
        int start = 0;

        while (start < s->length) {
            int end = start + 1;

            while (end < s->length &&
                   s->timeline[end] == s->timeline[start])
                end++;

            if (s->timeline[start] == 0)
                fprintf(out, "| IDLE (%d-%d) ", start, end);
            else
                fprintf(out, "| P%d (%d-%d) ",
                        s->timeline[start], start, end);

            start = end;
        }

        fprintf(out, "|\n");
    }

    fprintf(out, "\nPID\tAT\tBT\tPriority\tCT\tWT\tTAT\tRT\n");

    for (int i = 0; i < n; i++) {
        int tat = p[i].completion - p[i].arrival;
        int wt = tat - p[i].burst;
        int rt = p[i].first_start - p[i].arrival;

        fprintf(out, "P%d\t%d\t%d\t%d\t\t%d\t%d\t%d\t%d\n",
                p[i].pid, p[i].arrival, p[i].burst,
                p[i].priority, p[i].completion, wt, tat, rt);
    }

    fprintf(out, "\nAverage waiting time: %.2f\n",
            stats->avg_waiting);
    fprintf(out, "Average turnaround time: %.2f\n",
            stats->avg_turnaround);
    fprintf(out, "Average response time: %.2f\n",
            stats->avg_response);
}

void display_comparison(Statistics *normal, Statistics *aging) {
    printf("\n===== PERFORMANCE COMPARISON =====\n");
    printf("%-25s %12s %12s\n",
           "Metric", "Normal", "With Aging");

    printf("%-25s %12.2f %12.2f\n",
           "Avg waiting time", normal->avg_waiting,
           aging->avg_waiting);

    printf("%-25s %12.2f %12.2f\n",
           "Avg turnaround time", normal->avg_turnaround,
           aging->avg_turnaround);

    printf("%-25s %12.2f %12.2f\n",
           "Avg response time", normal->avg_response,
           aging->avg_response);

    printf("\nLower values are better for these time metrics.\n");
    printf("Aging does not guarantee improvement in every metric.\n");
}



void demonstrate_ipc(const char *program) {
    int fd[2];

    if (pipe(fd) == -1) {
        perror("pipe");
        return;
    }

    pid_t pid = fork();

    if (pid == -1) {
        perror("fork");
        close(fd[0]);
        close(fd[1]);
        return;
    }

    if (pid == 0) {
        close(fd[0]);

        if (dup2(fd[1], STDOUT_FILENO) == -1)
            _exit(1);

        close(fd[1]);

        execl(program, program, "--worker", (char *)NULL);

        perror("exec");
        _exit(1);
    }

    close(fd[1]);

    char buffer[128];
    ssize_t bytes;

    printf("\n===== IPC DEMONSTRATION =====\n");

    while ((bytes = read(fd[0], buffer, sizeof(buffer) - 1)) > 0) {
        buffer[bytes] = '\0';
        printf("Child says: %s", buffer);
    }

    close(fd[0]);

    int status;
    if (waitpid(pid, &status, 0) == -1)
        perror("waitpid");
}



int main(int argc, char *argv[]) {
    /* Child mode used by exec(). */
    if (argc > 1 && strcmp(argv[1], "--worker") == 0) {
        printf("Child process executed successfully.\n");
        return 0;
    }

    int n;

    printf("SMART CPU SCHEDULER WITH DYNAMIC PRIORITY\n");
    printf("=========================================\n");
    printf("Enter number of processes (1-%d): ", MAX_PROCESSES);

    if (scanf("%d", &n) != 1 || n < 1 || n > MAX_PROCESSES) {
        printf("Invalid process count.\n");
        return 1;
    }


    Process *original = malloc(n * sizeof(Process));

    if (original == NULL) {
        perror("malloc");
        return 1;
    }

    int max_arrival = 0;
    int total_burst = 0;

    for (int i = 0; i < n; i++) {
        original[i].pid = i + 1;

        printf("\nProcess P%d\n", i + 1);

        printf("Arrival time: ");
        if (scanf("%d", &original[i].arrival) != 1 ||
            original[i].arrival < 0 ||
            original[i].arrival > MAX_INPUT_TIME) {
            printf("Invalid arrival time.\n");
            free(original);
            return 1;
        }

        printf("Burst time: ");
        if (scanf("%d", &original[i].burst) != 1 ||
            original[i].burst < 1 ||
            original[i].burst > MAX_INPUT_TIME) {
            printf("Invalid burst time.\n");
            free(original);
            return 1;
        }

        printf("Initial priority (higher = higher priority): ");
        if (scanf("%d", &original[i].priority) != 1 ||
            original[i].priority < 0 ||
            original[i].priority > 100000) {
            printf("Invalid priority.\n");
            free(original);
            return 1;
        }

        if (original[i].arrival > max_arrival)
            max_arrival = original[i].arrival;

        total_burst += original[i].burst;
    }

    /*
     * Enough space for initial CPU idle time plus all bursts.
     * A small margin is included for safety.
     */
    int capacity = max_arrival + total_burst + 2;

    Process *normal = copy_processes(original, n);
    Process *aging = copy_processes(original, n);

    Schedule normal_schedule = {NULL, 0, capacity};
    Schedule aging_schedule = {NULL, 0, capacity};

    normal_schedule.timeline = calloc(capacity, sizeof(int));
    aging_schedule.timeline = calloc(capacity, sizeof(int));

    if (normal == NULL || aging == NULL ||
        normal_schedule.timeline == NULL ||
        aging_schedule.timeline == NULL) {
        perror("allocation");
        free(original);
        free(normal);
        free(aging);
        free(normal_schedule.timeline);
        free(aging_schedule.timeline);
        return 1;
    }

    printf("\nAging interval: %d time units\n", AGING_INTERVAL);

    if (run_scheduler(normal, n, &normal_schedule, 0) != 0 ||
        run_scheduler(aging, n, &aging_schedule, 1) != 0) {
        free(original);
        free(normal);
        free(aging);
        free(normal_schedule.timeline);
        free(aging_schedule.timeline);
        return 1;
    }

    Statistics normal_stats = {0, 0, 0};
    Statistics aging_stats = {0, 0, 0};

    if (get_statistics(normal, n, &normal_stats) != 0 ||
        get_statistics(aging, n, &aging_stats) != 0) {
        free(original);
        free(normal);
        free(aging);
        free(normal_schedule.timeline);
        free(aging_schedule.timeline);
        return 1;
    }

    display_results(stdout, "ORDINARY PRIORITY SCHEDULING",
                    normal, n, &normal_schedule, &normal_stats);

    display_results(stdout, "DYNAMIC PRIORITY SCHEDULING",
                    aging, n, &aging_schedule, &aging_stats);

    display_comparison(&normal_stats, &aging_stats);


    FILE *report = fopen("scheduler_report.txt", "w");

    if (report == NULL) {
        perror("fopen");
    } else {
        display_results(report, "ORDINARY PRIORITY SCHEDULING",
                        normal, n, &normal_schedule, &normal_stats);

        display_results(report, "DYNAMIC PRIORITY SCHEDULING",
                        aging, n, &aging_schedule, &aging_stats);

        display_comparison(&normal_stats, &aging_stats);
        fclose(report);

        printf("\nReport saved to scheduler_report.txt\n");
    }

    demonstrate_ipc(argv[0]);

    /* Release allocated memory. */
    free(original);
    free(normal);
    free(aging);
    free(normal_schedule.timeline);
    free(aging_schedule.timeline);

    pthread_mutex_destroy(&stats_mutex);

    return 0;
}

