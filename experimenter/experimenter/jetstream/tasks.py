from pathlib import Path

import markus
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

from experimenter.celery import app
from experimenter.experiments.changelog_utils import generate_nimbus_changelog
from experimenter.experiments.constants import NimbusConstants
from experimenter.experiments.models import NimbusChangeLog, NimbusExperiment
from experimenter.jetstream.client import (
    ERRORS_FOLDER,
    METADATA_FOLDER,
    STATISTICS_FOLDER,
    analysis_storage,
    get_enrollment_funnel_data,
    get_experiment_data,
    get_monitoring_data,
    get_population_sizing_data,
)
from experimenter.jetstream.models import AnalysisWindow
from experimenter.kinto.tasks import get_kinto_user

logger = get_task_logger(__name__)
metrics = markus.get_metrics("jetstream.tasks")


def strip_errors(data):
    """
    Strip errors from result data for meaningful comparison.

    Errors contain timestamps and other metadata that change on every fetch
    even when the actual analysis results are unchanged. We still store errors
    in the database, but don't use them to determine if results have changed.
    """
    if not data:
        return data

    return {
        version_key: {k: v for k, v in version_data.items() if k != "errors"}
        if isinstance(version_data, dict)
        else version_data
        for version_key, version_data in data.items()
    }


RESULTS_FOLDERS = [STATISTICS_FOLDER, METADATA_FOLDER, ERRORS_FOLDER]


def get_results_filenames():
    filenames_by_folder = {}
    for folder in RESULTS_FOLDERS:
        _, filenames = analysis_storage.listdir(folder)
        filenames_by_folder[folder] = set(filenames)
    return filenames_by_folder


def get_latest_results_timestamp(experiment_slug, results_filenames):
    recipe_slug = experiment_slug.replace("-", "_")
    expected_filenames = {
        STATISTICS_FOLDER: [
            f"statistics_{recipe_slug}_{window}.json" for window in AnalysisWindow
        ],
        METADATA_FOLDER: [f"metadata_{recipe_slug}.json"],
        ERRORS_FOLDER: [f"errors_{recipe_slug}.json"],
    }

    latest_timestamp = None
    for folder, filenames in expected_filenames.items():
        for filename in filenames:
            if filename not in results_filenames[folder]:
                continue

            path = Path(folder, filename)
            file_timestamp = analysis_storage.get_modified_time(str(path))

            if latest_timestamp is None or file_timestamp > latest_timestamp:
                latest_timestamp = file_timestamp

    return latest_timestamp


@app.task
@metrics.timer_decorator("fetch_experiment_data")
def fetch_experiment_data(experiment_id, results_data_updated_at=None):
    metrics.incr("fetch_experiment_data.started")
    experiment = None
    try:
        experiment = NimbusExperiment.objects.get(id=experiment_id)
        old_results_data = experiment.results_data
        new_results_data = get_experiment_data(experiment)

        if old_results_data != new_results_data:
            experiment.results_data = new_results_data

            old_normalized = strip_errors(old_results_data)
            new_normalized = strip_errors(new_results_data)

            if old_normalized != new_normalized:
                generate_nimbus_changelog(
                    experiment,
                    get_kinto_user(),
                    message=NimbusChangeLog.Messages.RESULTS_UPDATED,
                )

        if results_data_updated_at is not None:
            experiment.results_data_updated_at = results_data_updated_at

        if old_results_data != new_results_data or results_data_updated_at is not None:
            experiment.save()

        metrics.incr("fetch_experiment_data.completed")
    except Exception as e:
        metrics.incr("fetch_experiment_data.failed")
        failure_message = f"Fetching experiment data for {experiment_id} "
        if experiment is not None and hasattr(experiment, "slug"):
            failure_message += f"{experiment.slug} "
        failure_message += f"failed: {e}"
        logger.error(failure_message)
        raise e


@app.task
@metrics.timer_decorator("fetch_jetstream_data")
def fetch_jetstream_data():
    metrics.incr("fetch_jetstream_data.started")
    try:
        results_filenames = get_results_filenames()
        for experiment in NimbusExperiment.objects.filter(
            status__in=[NimbusExperiment.Status.COMPLETE, NimbusExperiment.Status.LIVE]
        ):
            latest_results_timestamp = get_latest_results_timestamp(
                experiment.slug, results_filenames
            )
            if latest_results_timestamp is None:
                metrics.incr("fetch_jetstream_data.skipped")
                continue

            if (
                experiment.results_data_updated_at is None
                or experiment.results_data_updated_at < latest_results_timestamp
            ):
                logger.info(
                    f"Fetching Jetstream data for {experiment.name} ({experiment.slug})"
                )
                fetch_experiment_data.delay(experiment.id, latest_results_timestamp)
                metrics.incr("fetch_jetstream_data.completed")
            else:
                logger.info(
                    f"Skipping cache refresh for experiment {experiment.name}"
                    f" ({experiment.slug}) because results data is up to date"
                )
                metrics.incr("fetch_jetstream_data.skipped")

    except Exception as e:
        metrics.incr("fetch_jetstream_data.failed")
        logger.error(f"Fetching Jetstream data failed: {e}")
        raise e


@app.task
@metrics.timer_decorator("fetch_population_sizing_data")
def fetch_population_sizing_data():
    metrics.incr("fetch_population_sizing_data.started")
    try:
        sizing_data = get_population_sizing_data()
        sizing = sizing_data.get("v1")

        if sizing is not None:
            cache.set(settings.SIZING_DATA_KEY, sizing.json())

        metrics.incr("fetch_population_sizing_data.completed")
    except Exception as e:
        metrics.incr("fetch_population_sizing_data.failed")
        logger.error(f"Fetching experiment population auto-sizing data failed: {e}")
        raise e


@app.task
@metrics.timer_decorator("fetch_monitoring_data")
def fetch_monitoring_data():
    metrics.incr("fetch_monitoring_data.started")
    try:
        data = get_monitoring_data()

        if not data or "v1" not in data:
            logger.error("No enrollment alert data found in GCS")
            metrics.incr("fetch_monitoring_data.failed")
            return

        alert_data = data.get("v1")

        try:
            funnel_data = get_enrollment_funnel_data()
            funnel_by_slug = funnel_data.get("v1", {}) if funnel_data else {}
        except Exception as e:
            logger.warning(f"Could not fetch enrollment funnel data: {e}")
            funnel_by_slug = {}

        updated_count = 0

        for exp_slug, monitoring_data in alert_data.items():
            try:
                experiment = NimbusExperiment.objects.get(
                    slug=exp_slug,
                    status=NimbusConstants.Status.LIVE,
                )

                merged = {
                    **monitoring_data,
                    "enrollment_funnel": funnel_by_slug.get(exp_slug, []),
                }

                if experiment.monitoring_data != merged:
                    experiment.monitoring_data = merged
                    experiment.monitoring_data_updated_at = timezone.now()
                    experiment.save(
                        update_fields=["monitoring_data", "monitoring_data_updated_at"]
                    )
                    generate_nimbus_changelog(
                        experiment,
                        get_kinto_user(),
                        message=NimbusChangeLog.Messages.MONITORING_DATA_UPDATED,
                    )
                    updated_count += 1

            except NimbusExperiment.DoesNotExist:
                logger.warning(f"Experiment {exp_slug} not found in database")
                continue
            except Exception as e:
                logger.error(f"Failed to update experiment {exp_slug}: {e}")
                continue

        logger.info(
            f"Successfully updated monitoring data for {updated_count} experiments"
        )
        metrics.incr("fetch_monitoring_data.completed")

    except Exception as e:
        metrics.incr("fetch_monitoring_data.failed")
        logger.exception(f"Fatal error in fetch_monitoring_data task: {e}")
        raise
