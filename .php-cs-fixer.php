<?php declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

$finder = Finder::create()
    ->files()
    ->in([__DIR__ . '/bin', __DIR__ . '/src/']);

return (new Config())
    ->setFinder($finder)
    ->setRiskyAllowed(true)
    ->setCacheFile(__DIR__ . '/var/cs-fixer/php_cs.cache')
    ->setRules(
        [
            '@Symfony' => true,
            '@Symfony:risky' => true,
        ]
    );
