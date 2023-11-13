#!/usr/bin/env php
<?php declare(strict_types=1);

use App\Command\FetchCommand;
use App\Command\ReportCommand;
use Symfony\Component\Console\Application;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__.'/../vendor/autoload.php';

(new Dotenv())->usePutenv()->load(__DIR__.'/../.env');

$application = new Application();
$application->add(new FetchCommand(getenv('GITHUB_TOKEN') ?: ''));
$application->add(new ReportCommand());
$application->run();
